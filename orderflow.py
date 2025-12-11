import time
import json
import pandas as pd
from datetime import datetime
from collections import deque
import logging
from typing import Dict, List, Optional
from dhanhq import DhanContext, dhanhq
import websocket
import threading
import struct

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared dictionary for live data
live_market_data = {}

# Shared dictionary for historical orderflow
orderflow_history = {}

class OrderFlowAnalyzer:
    def __init__(self, client_id: str, access_token: str):
        context = DhanContext(client_id=client_id, access_token=access_token)
        self.dhan = dhanhq(context)

        self.order_flow_history = deque(maxlen=1000)
        self.previous_book = None
        self.previous_traded_data = None
        self.signals = []

    # -------------------------------------------------------------------------
    # QUOTE-API MARKET DEPTH FETCHER (UNCHANGED)
    # -------------------------------------------------------------------------
    def get_market_depth(self, security_id: str, exchange_segment: str = "NSE_FNO") -> Optional[Dict]:
        try:
            security_id_int = int(security_id)

            if "FUT" in str(security_id).upper() or "OPT" in str(security_id).upper():
                exchange_segment = "NSE_FNO"

            securities = {exchange_segment: [security_id_int]}
            response = self.dhan.quote_data(securities)

            if response.get("status") != "success" or not response.get("data"):
                return None

            outer_data = response["data"]
            nested_data = outer_data.get("data", {})
            if exchange_segment not in nested_data:
                return None

            segment_data = nested_data[exchange_segment]
            security_data = segment_data.get(str(security_id)) or segment_data.get(str(security_id_int))

            return security_data

        except Exception as e:
            logger.error(f"Error fetching market depth: {e}")
            return None

    # -------------------------------------------------------------------------
    # PRIMARY TRADED QUANTITY EXTRACTOR
    # -------------------------------------------------------------------------
    def extract_traded_quantities(self, market_depth: Dict) -> Dict:
        try:
            traded_data = {
                'buy_quantity': float(market_depth.get('buy_quantity', 0)),
                'sell_quantity': float(market_depth.get('sell_quantity', 0)),
                'total_volume': float(market_depth.get('volume', 0)),
                'last_trade_time': market_depth.get('last_trade_time', ''),
                'timestamp': datetime.now().isoformat()
            }

            traded_data['net_traded'] = traded_data['buy_quantity'] - traded_data['sell_quantity']

            if traded_data['sell_quantity'] > 0:
                traded_data['buy_sell_ratio'] = traded_data['buy_quantity'] / traded_data['sell_quantity']
            else:
                traded_data['buy_sell_ratio'] = float('inf') if traded_data['buy_quantity'] > 0 else 1.0

            return traded_data

        except Exception as e:
            logger.error(f"Error extracting traded quantities: {e}")
            return {
                'buy_quantity': 0,
                'sell_quantity': 0,
                'total_volume': 0,
                'net_traded': 0,
                'buy_sell_ratio': 1.0,
                'last_trade_time': '',
                'timestamp': datetime.now().isoformat()
            }

    # -------------------------------------------------------------------------
    # DELTA CALCULATOR
    # -------------------------------------------------------------------------
    def calculate_traded_quantity_delta(self, current_traded: Dict, previous_traded: Dict) -> Dict:
        try:
            buy_qty_delta = current_traded['buy_quantity'] - previous_traded['buy_quantity']
            sell_qty_delta = current_traded['sell_quantity'] - previous_traded['sell_quantity']
            volume_delta = current_traded['total_volume'] - previous_traded['total_volume']

            net_trade_flow = buy_qty_delta - sell_qty_delta

            total_interval_volume = buy_qty_delta + sell_qty_delta
            if total_interval_volume > 0:
                buy_percentage = (buy_qty_delta / total_interval_volume) * 100
                sell_percentage = (sell_qty_delta / total_interval_volume) * 100
            else:
                buy_percentage = 50.0
                sell_percentage = 50.0

            if volume_delta > 0:
                buy_intensity = buy_qty_delta / volume_delta if volume_delta > 0 else 0.5
                sell_intensity = sell_qty_delta / volume_delta if volume_delta > 0 else 0.5
            else:
                buy_intensity = 0.5
                sell_intensity = 0.5

            flow_rate = 0
            try:
                current_time = datetime.fromisoformat(current_traded['timestamp'])
                previous_time = datetime.fromisoformat(previous_traded['timestamp'])
                time_diff = (current_time - previous_time).total_seconds()
                if time_diff > 0:
                    flow_rate = net_trade_flow / time_diff
            except:
                pass

            return {
                'buy_qty_delta': buy_qty_delta,
                'sell_qty_delta': sell_qty_delta,
                'volume_delta': volume_delta,
                'net_trade_flow': net_trade_flow,
                'buy_percentage': buy_percentage,
                'sell_percentage': sell_percentage,
                'buy_intensity': buy_intensity,
                'sell_intensity': sell_intensity,
                'flow_rate': flow_rate,
                'interval_volume': total_interval_volume
            }

        except Exception as e:
            logger.error(f"Error calculating traded quantity delta: {e}")
            return {
                'buy_qty_delta': 0,
                'sell_qty_delta': 0,
                'volume_delta': 0,
                'net_trade_flow': 0,
                'buy_percentage': 50.0,
                'sell_percentage': 50.0,
                'buy_intensity': 0.5,
                'sell_intensity': 0.5,
                'flow_rate': 0,
                'interval_volume': 0
            }

    # -------------------------------------------------------------------------
    # IMBALANCE, WEIGHTED PRICES, DEPTH METRICS (unchanged)
    # -------------------------------------------------------------------------
    def calculate_imbalance_ratio(self, market_depth: Dict) -> float:
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            if total_ask_qty == 0:
                return float('inf')
            return total_bid_qty / total_ask_qty
        except:
            return 1.0

    def calculate_weighted_prices(self, market_depth: Dict) -> Dict[str, float]:
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            total_bid_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) for level in bid_levels)
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            total_ask_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) for level in ask_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            return {
                'weighted_bid': total_bid_value / total_bid_qty if total_bid_qty > 0 else 0,
                'weighted_ask': total_ask_value / total_ask_qty if total_ask_qty > 0 else 0,
                'spread': (total_ask_value / total_ask_qty if total_ask_qty > 0 else 0) -
                          (total_bid_value / total_bid_qty if total_bid_qty > 0 else 0)
            }
        except:
            return {'weighted_bid': 0, 'weighted_ask': 0, 'spread': 0}

    # -------------------------------------------------------------------------
    # MAIN ORDERFLOW PROCESSOR (UNCHANGED)
    # -------------------------------------------------------------------------
    def process_order_flow(self, security_id: str, exchange_segment: str = "NSE_FNO") -> Optional[Dict]:
        try:
            current_book = self.get_market_depth(security_id, exchange_segment)
            if not current_book:
                return None

            current_traded = self.extract_traded_quantities(current_book)

            traded_delta = {}
            if self.previous_traded_data:
                traded_delta = self.calculate_traded_quantity_delta(current_traded, self.previous_traded_data)

            imbalance_ratio = self.calculate_imbalance_ratio(current_book)
            weighted_prices = self.calculate_weighted_prices(current_book)

            flow_data = {
                'timestamp': datetime.now().isoformat(),
                'security_id': security_id,
                'ltp': current_book.get('ltp') or current_book.get('last_price', 0),
                'traded_quantities': current_traded,
                'traded_delta': traded_delta,
                'imbalance_ratio': imbalance_ratio,
                'weighted_prices': weighted_prices,
            }

            self.order_flow_history.append(flow_data)
            self.previous_traded_data = current_traded

            return flow_data

        except Exception as e:
            logger.error(f"Error processing order flow: {e}")
            return None

# -----------------------------------------------------------------------------
# 🧠 **FULL PATCHED WEBSOCKET CLIENT (THIS IS WHERE Dhan FULL PACKET IS PARSED)**
# -----------------------------------------------------------------------------
class DhanWebSocketClient:
    def __init__(self, access_token, client_id, instrument_list, data_store):
        self.access_token = access_token
        self.client_id = client_id
        self.instrument_list = instrument_list
        self.ws_url = (
            f"wss://api-feed.dhan.co?version=2&token={access_token}"
            f"&clientId={client_id}&authType=2"
        )
        self.ws = None
        self.data_store = data_store

    # -------------------------------------------------------------------------
    # 📌 **FULL PACKET PARSER ADDED HERE**
    # -------------------------------------------------------------------------
    def on_message(self, ws, message):
        if len(message) < 8:
            return

        code, msg_len, segment, security_id = struct.unpack(">BHB I", message[:8])

        try:
            # ---------------------------------------------------------------
            # FULL PACKET: CODE = 8
            # ---------------------------------------------------------------
            if code == 8 and len(message) >= 162:
                # Parse Full Packet according to Dhan documentation
                ltp, = struct.unpack(">f", message[8:12])
                ltq, = struct.unpack(">h", message[12:14])
                ltt, = struct.unpack(">I", message[14:18])
                atp, = struct.unpack(">f", message[18:22])
                volume, = struct.unpack(">I", message[22:26])
                sell_qty, = struct.unpack(">I", message[26:30])
                buy_qty, = struct.unpack(">I", message[30:34])
                oi, = struct.unpack(">I", message[34:38])
                high_oi, = struct.unpack(">I", message[38:42])
                low_oi, = struct.unpack(">I", message[42:46])
                day_open, = struct.unpack(">f", message[46:50])
                day_close, = struct.unpack(">f", message[50:54])
                day_high, = struct.unpack(">f", message[54:58])
                day_low, = struct.unpack(">f", message[58:62])

                # --- Parse Market Depth (5 levels)
                depth = {"buy": [], "sell": []}
                offset = 62
                for i in range(5):
                    bid_qty, ask_qty, bid_orders, ask_orders, bid_price, ask_price = \
                        struct.unpack(">I I h h f f", message[offset:offset+20])

                    depth["buy"].append({
                        "quantity": bid_qty,
                        "orders": bid_orders,
                        "price": bid_price
                    })
                    depth["sell"].append({
                        "quantity": ask_qty,
                        "orders": ask_orders,
                        "price": ask_price
                    })

                    offset += 20

                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "ltp": ltp,
                    "ltq": ltq,
                    "ltt": ltt,
                    "atp": atp,
                    "volume": volume,
                    "sell_qty": sell_qty,
                    "buy_qty": buy_qty,
                    "oi": oi,
                    "oi_day_high": high_oi,
                    "oi_day_low": low_oi,
                    "day_open": day_open,
                    "day_close": day_close,
                    "day_high": day_high,
                    "day_low": day_low,
                    "depth": depth
                }

                self.data_store[str(security_id)] = entry
                orderflow_history.setdefault(str(security_id), []).append(entry)

                print(f"[FULL] {security_id} | LTP={ltp} | OI={oi} | Buy={buy_qty} | Sell={sell_qty}")
                return

            # ---------------------------------------------------------------
            # QUOTE PACKET (code = 4)
            # ---------------------------------------------------------------
            if code == 4:
                ltp, = struct.unpack(">f", message[8:12])
                last_traded_qty, = struct.unpack(">h", message[12:14])
                ltt, = struct.unpack(">I", message[14:18])
                atp, = struct.unpack(">f", message[18:22])
                volume, = struct.unpack(">I", message[22:26])
                sell_qty, = struct.unpack(">I", message[26:30])
                buy_qty, = struct.unpack(">I", message[30:34])
                day_open, = struct.unpack(">f", message[34:38])
                day_close, = struct.unpack(">f", message[38:42])
                day_high, = struct.unpack(">f", message[42:46])
                day_low, = struct.unpack(">f", message[46:50])

                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "ltp": ltp,
                    "last_traded_qty": last_traded_qty,
                    "ltt": ltt,
                    "atp": atp,
                    "volume": volume,
                    "sell_qty": sell_qty,
                    "buy_qty": buy_qty,
                    "day_open": day_open,
                    "day_close": day_close,
                    "day_high": day_high,
                    "day_low": day_low
                }

                self.data_store[str(security_id)] = entry
                orderflow_history.setdefault(str(security_id), []).append(entry)
                print(f"[QUOTE] {security_id} LTP={ltp} Vol={volume}")
                return

            # ---------------------------------------------------------------
            # TICKER PACKET (code = 2)
            # ---------------------------------------------------------------
            if code == 2 and len(message) >= 16:
                ltp, = struct.unpack(">f", message[8:12])
                ltt, = struct.unpack(">I", message[12:16])
                print(f"[TICKER] {security_id} LTP={ltp} LTT={ltt}")
                return

            # ---------------------------------------------------------------
            # OI ONLY PACKET
            # ---------------------------------------------------------------
            if code == 5 and len(message) >= 12:
                oi, = struct.unpack(">I", message[8:12])
                print(f"[OI] {security_id} OI={oi}")
                return

            print(f"[UNKNOWN] Code={code} Length={len(message)}")

        except Exception as e:
            print("[WebSocket] Error parsing message:", e)

    def on_error(self, ws, error):
        print("[WebSocket] Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[WebSocket] Closed {close_status_code} | {close_msg}")

    def on_open(self, ws):
        batch_size = 100
        for i in range(0, len(self.instrument_list), batch_size):
            batch = self.instrument_list[i:i+batch_size]
            subscribe_message = {
                "RequestCode": 17,
                "InstrumentCount": len(batch),
                "InstrumentList": batch
            }
            ws.send(json.dumps(subscribe_message))
            print(f"[WebSocket] Subscribed to {len(batch)} instruments.")

    def run(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        t = threading.Thread(target=self.ws.run_forever)
        t.daemon = True
        t.start()
        print("[WebSocket] Client started.")

# -----------------------------------------------------------------------------
# MAIN DEMO
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    CLIENT_ID = "1100244268"
    ACCESS_TOKEN = "REPLACE_ME"

    analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)

    # FULL PACKET TEST
    print("\n=== WebSocket FULL PACKET MODE ===\n")
