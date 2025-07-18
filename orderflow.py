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

# Shared dictionary for live data (thread-safe for simple use)
live_market_data = {}

# Shared dictionary for all order flow history
orderflow_history = {}

class OrderFlowAnalyzer:
    def __init__(self, client_id: str, access_token: str):
        """
        Initialize Dhan Order Flow Analyzer
        
        Args:
            client_id: Your Dhan client ID
            access_token: Your Dhan access token
        """
        context = DhanContext(client_id=client_id, access_token=access_token)
        self.dhan = dhanhq(context)

        self.order_flow_history = deque(maxlen=1000)  # Store last 1000 snapshots
        self.previous_book = None
        self.previous_traded_data = None  # Store previous traded quantities
        self.signals = []
        
    def get_market_depth(self, security_id: str, exchange_segment: str = "NSE_FNO") -> Optional[Dict]:
        try:
            # Convert security_id to integer for API call
            security_id_int = int(security_id)

            # 🔥 Auto-detect exchange segment for F&O instruments
            if "FUT" in str(security_id).upper() or "OPT" in str(security_id).upper():
                exchange_segment = "NSE_FNO"

            securities = {exchange_segment: [security_id_int]}
            print(f"🔍 Requesting data: {securities}")  # Debug log

            response = self.dhan.quote_data(securities)
            print("📦 Raw response from Dhan:\n", json.dumps(response, indent=2))

            # ✅ Check if API succeeded
            if response.get("status") != "success" or not response.get("data"):
                print(f"⚠️ API failure: {response.get('remarks')}")
                return None

            outer_data = response["data"]
            nested_data = outer_data.get("data", {})
            if exchange_segment not in nested_data:
                print(f"⚠️ Exchange segment '{exchange_segment}' not found in response")
                print(f"Available segments: {list(nested_data.keys())}")
                return None

            segment_data = nested_data[exchange_segment]
            security_data = segment_data.get(str(security_id)) or segment_data.get(str(security_id_int))

            if not security_data:
                print(f"⚠️ Security ID '{security_id}' not found in {exchange_segment}")
                print(f"Available IDs: {list(segment_data.keys())}")
                return None

            print(f"✅ Successfully retrieved data for {security_id} on {exchange_segment}")
            if response and isinstance(response, dict):
                print("Received:", response)
            return security_data

        except ValueError as e:
            logger.error(f"Invalid security_id format '{security_id}': {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching market depth for {security_id} on {exchange_segment}: {e}")
        return None

    def extract_traded_quantities(self, market_depth: Dict) -> Dict:
        """
        Extract traded quantity data from market depth response
        
        Args:
            market_depth: Market depth data from API
            
        Returns:
            Dictionary with buy_quantity, sell_quantity, and volume data
        """
        try:
            traded_data = {
                'buy_quantity': float(market_depth.get('buy_quantity', 0)),
                'sell_quantity': float(market_depth.get('sell_quantity', 0)),
                'total_volume': float(market_depth.get('volume', 0)),
                'last_trade_time': market_depth.get('last_trade_time', ''),
                'timestamp': datetime.now().isoformat()
            }
            
            # Calculate net traded quantity (buy - sell)
            traded_data['net_traded'] = traded_data['buy_quantity'] - traded_data['sell_quantity']
            
            # Calculate buy/sell ratio
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

    def calculate_traded_quantity_delta(self, current_traded: Dict, previous_traded: Dict) -> Dict:
        """
        Calculate delta in traded quantities between snapshots
        
        Args:
            current_traded: Current traded quantity data
            previous_traded: Previous traded quantity data
            
        Returns:
            Dictionary with traded quantity deltas and flow metrics
        """
        try:
            # Calculate deltas in cumulative traded quantities
            buy_qty_delta = current_traded['buy_quantity'] - previous_traded['buy_quantity']
            sell_qty_delta = current_traded['sell_quantity'] - previous_traded['sell_quantity']
            volume_delta = current_traded['total_volume'] - previous_traded['total_volume']
            
            # Net flow from actual trades (positive = more buying, negative = more selling)
            net_trade_flow = buy_qty_delta - sell_qty_delta
            
            # Calculate percentage of volume that was buying vs selling in this interval
            total_interval_volume = buy_qty_delta + sell_qty_delta
            if total_interval_volume > 0:
                buy_percentage = (buy_qty_delta / total_interval_volume) * 100
                sell_percentage = (sell_qty_delta / total_interval_volume) * 100
            else:
                buy_percentage = 50.0
                sell_percentage = 50.0
            
            # Trade intensity (how much of total volume change came from buying vs selling)
            if volume_delta > 0:
                buy_intensity = buy_qty_delta / volume_delta if volume_delta > 0 else 0.5
                sell_intensity = sell_qty_delta / volume_delta if volume_delta > 0 else 0.5
            else:
                buy_intensity = 0.5
                sell_intensity = 0.5
            
            # Calculate time-based flow rate (if we have time data)
            flow_rate = 0
            try:
                current_time = datetime.fromisoformat(current_traded['timestamp'])
                previous_time = datetime.fromisoformat(previous_traded['timestamp'])
                time_diff = (current_time - previous_time).total_seconds()
                if time_diff > 0:
                    flow_rate = net_trade_flow / time_diff  # Flow per second
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

    def calculate_imbalance_ratio(self, market_depth: Dict) -> float:
        """
        Calculate bid-ask imbalance ratio from order book
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Imbalance ratio (>1 = buying pressure, <1 = selling pressure)
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            
            if total_ask_qty == 0:
                return float('inf')
            
            return total_bid_qty / total_ask_qty
        except Exception as e:
            logger.error(f"Error calculating imbalance: {e}")
            return 1.0
    
    def calculate_weighted_prices(self, market_depth: Dict) -> Dict[str, float]:
        """
        Calculate volume-weighted bid and ask prices
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Dictionary with weighted bid and ask prices
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Calculate weighted bid price
            total_bid_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) 
                                for level in bid_levels)
            total_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels)
            
            # Calculate weighted ask price
            total_ask_value = sum(float(level.get('price', 0)) * float(level.get('quantity', 0)) 
                                for level in ask_levels)
            total_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels)
            
            weighted_bid = total_bid_value / total_bid_qty if total_bid_qty > 0 else 0
            weighted_ask = total_ask_value / total_ask_qty if total_ask_qty > 0 else 0
            
            return {
                'weighted_bid': weighted_bid,
                'weighted_ask': weighted_ask,
                'spread': weighted_ask - weighted_bid
            }
        except Exception as e:
            logger.error(f"Error calculating weighted prices: {e}")
            return {'weighted_bid': 0, 'weighted_ask': 0, 'spread': 0}
    
    def calculate_order_book_delta(self, current_book: Dict, previous_book: Dict) -> Dict:
        """
        Calculate order book changes between snapshots (secondary metric)
        
        Args:
            current_book: Current market depth
            previous_book: Previous market depth
            
        Returns:
            Dictionary with bid/ask deltas
        """
        try:
            # Current totals
            current_bid_qty = sum(float(level.get('quantity', 0)) 
                                for level in current_book.get('depth', {}).get('buy', []))
            current_ask_qty = sum(float(level.get('quantity', 0)) 
                                for level in current_book.get('depth', {}).get('sell', []))
            
            # Previous totals
            previous_bid_qty = sum(float(level.get('quantity', 0)) 
                                 for level in previous_book.get('depth', {}).get('buy', []))
            previous_ask_qty = sum(float(level.get('quantity', 0)) 
                                 for level in previous_book.get('depth', {}).get('sell', []))
            
            bid_delta = current_bid_qty - previous_bid_qty
            ask_delta = current_ask_qty - previous_ask_qty
            net_flow = bid_delta - ask_delta
            
            return {
                'order_bid_delta': bid_delta,
                'order_ask_delta': ask_delta,
                'order_net_flow': net_flow
            }
        except Exception as e:
            logger.error(f"Error calculating order book delta: {e}")
            return {'order_bid_delta': 0, 'order_ask_delta': 0, 'order_net_flow': 0}
    
    def detect_large_orders(self, market_depth: Dict, threshold_multiplier: float = 2.0) -> Dict:
        """
        Detect unusually large orders in the book
        
        Args:
            market_depth: Market depth data
            threshold_multiplier: Multiplier for average size to detect large orders
            
        Returns:
            Dictionary with large order detection results
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Calculate average quantities
            bid_quantities = [float(level.get('quantity', 0)) for level in bid_levels]
            ask_quantities = [float(level.get('quantity', 0)) for level in ask_levels]
            
            avg_bid_qty = sum(bid_quantities) / len(bid_quantities) if bid_quantities else 0
            avg_ask_qty = sum(ask_quantities) / len(ask_quantities) if ask_quantities else 0
            
            # Detect large orders
            large_bids = [qty for qty in bid_quantities if qty > avg_bid_qty * threshold_multiplier]
            large_asks = [qty for qty in ask_quantities if qty > avg_ask_qty * threshold_multiplier]
            
            return {
                'large_bid_count': len(large_bids),
                'large_ask_count': len(large_asks),
                'max_bid_size': max(bid_quantities) if bid_quantities else 0,
                'max_ask_size': max(ask_quantities) if ask_quantities else 0,
                'avg_bid_size': avg_bid_qty,
                'avg_ask_size': avg_ask_qty
            }
        except Exception as e:
            logger.error(f"Error detecting large orders: {e}")
            return {'large_bid_count': 0, 'large_ask_count': 0, 'max_bid_size': 0, 'max_ask_size': 0}
    
    def analyze_depth_levels(self, market_depth: Dict) -> Dict:
        """
        Analyze order distribution across different depth levels
        
        Args:
            market_depth: Market depth data
            
        Returns:
            Dictionary with depth analysis
        """
        try:
            bid_levels = market_depth.get('depth', {}).get('buy', [])
            ask_levels = market_depth.get('depth', {}).get('sell', [])
            
            # Top 5 levels vs deeper levels
            top5_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels[:5])
            deep_bid_qty = sum(float(level.get('quantity', 0)) for level in bid_levels[5:])
            
            top5_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels[:5])
            deep_ask_qty = sum(float(level.get('quantity', 0)) for level in ask_levels[5:])
            
            # Calculate ratios
            bid_depth_ratio = top5_bid_qty / deep_bid_qty if deep_bid_qty > 0 else float('inf')
            ask_depth_ratio = top5_ask_qty / deep_ask_qty if deep_ask_qty > 0 else float('inf')
            
            return {
                'bid_depth_ratio': bid_depth_ratio,
                'ask_depth_ratio': ask_depth_ratio,
                'top5_bid_qty': top5_bid_qty,
                'top5_ask_qty': top5_ask_qty,
                'deep_bid_qty': deep_bid_qty,
                'deep_ask_qty': deep_ask_qty
            }
        except Exception as e:
            logger.error(f"Error analyzing depth levels: {e}")
            return {'bid_depth_ratio': 0, 'ask_depth_ratio': 0}
    
    def generate_order_flow_signals(self, flow_data: Dict) -> str:
        """
        Generate trading signals based on order flow analysis (now primarily using traded quantities)
        
        Args:
            flow_data: Processed order flow data
            
        Returns:
            Signal string (BULLISH_FLOW, BEARISH_FLOW, NEUTRAL_FLOW)
        """
        try:
            # Primary signals from traded quantities
            net_trade_flow = flow_data.get('traded_delta', {}).get('net_trade_flow', 0)
            buy_percentage = flow_data.get('traded_delta', {}).get('buy_percentage', 50)
            buy_intensity = flow_data.get('traded_delta', {}).get('buy_intensity', 0.5)
            
            # Secondary signals from order book
            imbalance = flow_data.get('imbalance_ratio', 1.0)
            large_bid_count = flow_data.get('large_orders', {}).get('large_bid_count', 0)
            large_ask_count = flow_data.get('large_orders', {}).get('large_ask_count', 0)
            
            # Signal generation logic
            bullish_signals = 0
            bearish_signals = 0
            
            # PRIMARY: Traded quantity-based signals (weighted heavily)
            if net_trade_flow > 0:
                bullish_signals += 3  # Strong weight for actual trades
            elif net_trade_flow < 0:
                bearish_signals += 3
            
            # Buy percentage signals
            if buy_percentage > 60:
                bullish_signals += 2
            elif buy_percentage < 40:
                bearish_signals += 2
            
            # Buy intensity signals
            if buy_intensity > 0.6:
                bullish_signals += 2
            elif buy_intensity < 0.4:
                bearish_signals += 2
            
            # SECONDARY: Order book-based signals (lower weight)
            if imbalance > 1.5:
                bullish_signals += 1
            elif imbalance < 0.67:
                bearish_signals += 1
            
            # Large order signals
            if large_bid_count > large_ask_count:
                bullish_signals += 1
            elif large_ask_count > large_bid_count:
                bearish_signals += 1
            
            # Generate final signal with stronger thresholds due to higher weights
            if bullish_signals > bearish_signals + 2:
                return "BULLISH_FLOW"
            elif bearish_signals > bullish_signals + 2:
                return "BEARISH_FLOW"
            else:
                return "NEUTRAL_FLOW"
                
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return "NEUTRAL_FLOW"
    
    def process_order_flow(self, security_id: str, exchange_segment: str = "NSE_FNO") -> Optional[Dict]:
        """
        Process complete order flow analysis for a security (enhanced with traded quantities)
        
        Args:
            security_id: Security identifier
            exchange_segment: Exchange segment
            
        Returns:
            Complete order flow analysis or None if error
        """
        try:
            # Get current market depth
            current_book = self.get_market_depth(security_id, exchange_segment)
            if not current_book:
                return None
            
            # Extract traded quantities (PRIMARY DATA)
            current_traded = self.extract_traded_quantities(current_book)
            
            # Calculate traded quantity deltas if we have previous data
            traded_delta = {}
            if self.previous_traded_data:
                traded_delta = self.calculate_traded_quantity_delta(current_traded, self.previous_traded_data)
            
            # Calculate secondary metrics from order book
            imbalance_ratio = self.calculate_imbalance_ratio(current_book)
            weighted_prices = self.calculate_weighted_prices(current_book)
            large_orders = self.detect_large_orders(current_book)
            depth_analysis = self.analyze_depth_levels(current_book)
            
            # Calculate order book deltas (secondary data)
            order_delta = {}
            if self.previous_book:
                order_delta = self.calculate_order_book_delta(current_book, self.previous_book)
            
            # Compile flow data
            flow_data = {
                'timestamp': datetime.now().isoformat(),
                'security_id': security_id,
                'ltp': current_book.get('ltp') or current_book.get('last_price', 0),
                
                # PRIMARY: Traded quantity data
                'traded_quantities': current_traded,
                'traded_delta': traded_delta,
                
                # SECONDARY: Order book data
                'imbalance_ratio': imbalance_ratio,
                'weighted_prices': weighted_prices,
                'large_orders': large_orders,
                'depth_analysis': depth_analysis,
                'order_book_delta': order_delta
            }
            
            # Generate signal (now primarily based on traded quantities)
            signal = self.generate_order_flow_signals(flow_data)
            flow_data['signal'] = signal
            
            # Store data
            self.order_flow_history.append(flow_data)
            self.previous_book = current_book
            self.previous_traded_data = current_traded
            
            return flow_data
            
        except Exception as e:
            logger.error(f"Error processing order flow: {e}")
            return None
    
    def run_continuous_monitoring(self, security_id: str, exchange_segment: str = "NSE_FNO", 
                                 interval: int = 1, duration: int = 3600):
        """
        Run continuous order flow monitoring with enhanced traded quantity tracking
        
        Args:
            security_id: Security identifier
            exchange_segment: Exchange segment
            interval: Monitoring interval in seconds
            duration: Total monitoring duration in seconds
        """
        logger.info(f"Starting continuous monitoring for {security_id}")
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration:
                flow_data = self.process_order_flow(security_id, exchange_segment)
                
                if flow_data:
                    # Extract key metrics for logging
                    traded_delta = flow_data.get('traded_delta', {})
                    net_trade_flow = traded_delta.get('net_trade_flow', 0)
                    buy_percentage = traded_delta.get('buy_percentage', 50)
                    
                    # Log key metrics
                    logger.info(f"Time: {flow_data['timestamp'][:19]} | "
                              f"LTP: {flow_data['ltp']:.2f} | "
                              f"Net Trade Flow: {net_trade_flow:.0f} | "
                              f"Buy%: {buy_percentage:.1f}% | "
                              f"Imbalance: {flow_data['imbalance_ratio']:.2f} | "
                              f"Signal: {flow_data['signal']}")
                    
                    # Alert on strong signals
                    if flow_data['signal'] in ['BULLISH_FLOW', 'BEARISH_FLOW']:
                        logger.warning(f"🚨 ALERT: {flow_data['signal']} detected! "
                                     f"Net Trade Flow: {net_trade_flow:.0f}, "
                                     f"Buy Percentage: {buy_percentage:.1f}%")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")
    
    def get_flow_summary(self, lookback_minutes: int = 30) -> Dict:
        """
        Get summary of order flow over specified time period (enhanced with traded quantities)
        
        Args:
            lookback_minutes: Minutes to look back
            
        Returns:
            Summary statistics
        """
        try:
            cutoff_time = datetime.now().timestamp() - (lookback_minutes * 60)
            
            recent_data = [
                data for data in self.order_flow_history
                if datetime.fromisoformat(data['timestamp']).timestamp() > cutoff_time
            ]
            
            if not recent_data:
                return {}
            
            # Calculate summary statistics
            avg_imbalance = sum(d['imbalance_ratio'] for d in recent_data) / len(recent_data)
            
            # Traded quantity summaries
            total_net_trade_flow = sum(d.get('traded_delta', {}).get('net_trade_flow', 0) for d in recent_data)
            avg_buy_percentage = sum(d.get('traded_delta', {}).get('buy_percentage', 50) for d in recent_data) / len(recent_data)
            
            signal_counts = {}
            for data in recent_data:
                signal = data['signal']
                signal_counts[signal] = signal_counts.get(signal, 0) + 1
            
            return {
                'period_minutes': lookback_minutes,
                'data_points': len(recent_data),
                'avg_imbalance_ratio': avg_imbalance,
                'total_net_trade_flow': total_net_trade_flow,
                'avg_buy_percentage': avg_buy_percentage,
                'signal_distribution': signal_counts,
                'dominant_signal': max(signal_counts.items(), key=lambda x: x[1])[0] if signal_counts else 'NEUTRAL_FLOW'
            }
            
        except Exception as e:
            logger.error(f"Error generating flow summary: {e}")
            return {}
    
    def export_data_to_csv(self, filename: str = None):
        """
        Export order flow data to CSV (enhanced with traded quantity data)
        
        Args:
            filename: Output filename (optional)
        """
        try:
            if not filename:
                filename = f"order_flow_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Convert to DataFrame
            df_data = []
            for data in self.order_flow_history:
                traded_delta = data.get('traded_delta', {})
                traded_quantities = data.get('traded_quantities', {})
                
                row = {
                    'timestamp': data['timestamp'],
                    'security_id': data['security_id'],
                    'ltp': data['ltp'],
                    'signal': data['signal'],
                    
                    # Primary traded quantity metrics
                    'net_trade_flow': traded_delta.get('net_trade_flow', 0),
                    'buy_qty_delta': traded_delta.get('buy_qty_delta', 0),
                    'sell_qty_delta': traded_delta.get('sell_qty_delta', 0),
                    'buy_percentage': traded_delta.get('buy_percentage', 50),
                    'buy_intensity': traded_delta.get('buy_intensity', 0.5),
                    'cumulative_buy_qty': traded_quantities.get('buy_quantity', 0),
                    'cumulative_sell_qty': traded_quantities.get('sell_quantity', 0),
                    'buy_sell_ratio': traded_quantities.get('buy_sell_ratio', 1.0),
                    
                    # Secondary order book metrics
                    'imbalance_ratio': data['imbalance_ratio'],
                    'weighted_bid': data['weighted_prices']['weighted_bid'],
                    'weighted_ask': data['weighted_prices']['weighted_ask'],
                    'spread': data['weighted_prices']['spread'],
                    'large_bid_count': data['large_orders']['large_bid_count'],
                    'large_ask_count': data['large_orders']['large_ask_count']
                }
                df_data.append(row)
            
            df = pd.DataFrame(df_data)
            df.to_csv(filename, index=False)
            logger.info(f"Data exported to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")

class DhanWebSocketClient:
    def __init__(self, access_token, client_id, instrument_list, data_store):
        self.access_token = access_token
        self.client_id = client_id
        self.instrument_list = instrument_list  # List of dicts: {"ExchangeSegment": ..., "SecurityId": ...}
        self.ws_url = f"wss://api-feed.dhan.co?version=2&token={access_token}&clientId={client_id}&authType=2"
        self.ws = None
        self.data_store = data_store

    def on_message(self, ws, message):
        print("[WebSocket] Received message of length:", len(message))
        if len(message) < 8:
            return
        code, msg_len, segment, security_id = struct.unpack(">BHB I", message[:8])
        print(f"[WebSocket] Header: code={code}, msg_len={msg_len}, segment={segment}, security_id={security_id}")
        try:
            if code == 4 and len(message) >= 50:
                # Quote Packet
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
                if str(security_id) not in orderflow_history:
                    orderflow_history[str(security_id)] = []
                orderflow_history[str(security_id)].append(entry)
                print(f"[WebSocket] Updated {security_id}: LTP={ltp}, Vol={volume}, Buy={buy_qty}, Sell={sell_qty}, Open={day_open}, Close={day_close}, High={day_high}, Low={day_low}")
            elif code == 2 and len(message) >= 16:
                # Ticker Packet
                ltp, = struct.unpack(">f", message[8:12])
                ltt, = struct.unpack(">I", message[12:16])
                print(f"[WebSocket] Ticker: {security_id} LTP={ltp} LTT={ltt}")
            elif code == 5 and len(message) >= 12:
                # OI Data
                oi, = struct.unpack(">I", message[8:12])
                print(f"[WebSocket] OI: {security_id} OI={oi}")
            else:
                print(f"[WebSocket] Unhandled code {code} or unexpected length {len(message)}")
        except Exception as e:
            print("[WebSocket] Error parsing message:", e)

    def on_error(self, ws, error):
        print("[WebSocket] Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[WebSocket] Closed: {close_status_code} {close_msg}")

    def on_open(self, ws):
        # Send subscription message (max 100 instruments per message)
        batch_size = 100
        for i in range(0, len(self.instrument_list), batch_size):
            batch = self.instrument_list[i:i+batch_size]
            subscribe_message = {
                "RequestCode": 17,
                "InstrumentCount": len(batch),
                "InstrumentList": batch
            }
            ws.send(json.dumps(subscribe_message))
            print(f"[WebSocket] Sent subscription for {len(batch)} instruments.")

    def run(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        wst = threading.Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()
        print("[WebSocket] Client started.")

# Example usage with enhanced traded quantity tracking
if __name__ == "__main__":
    # Initialize analyzer
    CLIENT_ID = "1100244268"
    ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzUxMDA2OTE4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.caSAnGLGTZ0PSNcj0ICBfIQ9FgIxR68h8JHela-P151EQO9QucJ4KOfNEyGBwFtyEGPCBkBuQN2JyiYD0QzuSQ"
    
    analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
    
    # Test different exchanges
    test_securities = [
        ("53216", "NSE_FNO"),  # Nifty futures
        ("1333", "NSE_EQ"),    # HDFC Bank equity
    ]
    
    for security_id, exchange in test_securities:
        print(f"\n=== Testing {security_id} on {exchange} ===")
        result = analyzer.process_order_flow(security_id, exchange)
        if result:
            traded_delta = result.get('traded_delta', {})
            print(f"✅ Success: LTP={result.get('ltp')}, Signal={result.get('signal')}")
            print(f"   Net Trade Flow: {traded_delta.get('net_trade_flow', 0):.0f}")
            print(f"   Buy Percentage: {traded_delta.get('buy_percentage', 50):.1f}%")
        else:
            print(f"❌ Failed to get data for {security_id} on {exchange}")
    
    # Example: Run monitoring for 60 seconds to see traded quantity deltas
    print("\n=== Running 60-second monitoring demo ===")
    analyzer.run_continuous_monitoring("1333", "NSE_EQ", interval=2, duration=60)