import json
import struct
import threading
import websocket
from datetime import datetime
from flask import Flask, jsonify

app = Flask(__name__)

# This dictionary will store all live packets
live_data = {}
orderflow_history = {}

# -------------------------------------------------------------------
# 🧠 FULL PACKET PARSER (Code = 8)
# -------------------------------------------------------------------
def parse_full_packet(message):
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

    # Market Depth (5 levels)
    depth = {"buy": [], "sell": []}
    offset = 62

    for _ in range(5):
        bid_qty, ask_qty, bid_orders, ask_orders, bid_price, ask_price = \
            struct.unpack(">I I h h f f", message[offset:offset + 20])

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

    return {
        "timestamp": datetime.now().isoformat(),
        "type": "full",
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

# -------------------------------------------------------------------
# QUOTE PACKET (Code = 4)
# -------------------------------------------------------------------
def parse_quote_packet(message):
    ltp, = struct.unpack(">f", message[8:12])
    ltq, = struct.unpack(">h", message[12:14])
    ltt, = struct.unpack(">I", message[14:18])
    atp, = struct.unpack(">f", message[18:22])
    volume, = struct.unpack(">I", message[22:26])
    sell_qty, = struct.unpack(">I", message[26:30])
    buy_qty, = struct.unpack(">I", message[30:34])
    day_open, = struct.unpack(">f", message[34:38])
    day_close, = struct.unpack(">f", message[38:42])
    day_high, = struct.unpack(">f", message[42:46])
    day_low, = struct.unpack(">f", message[46:50])

    return {
        "timestamp": datetime.now().isoformat(),
        "type": "quote",
        "ltp": ltp,
        "ltq": ltq,
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

# -------------------------------------------------------------------
# TICKER PACKET (Code = 2)
# -------------------------------------------------------------------
def parse_ticker_packet(message):
    ltp, = struct.unpack(">f", message[8:12])
    ltt, = struct.unpack(">I", message[12:16])
    return {
        "timestamp": datetime.now().isoformat(),
        "type": "ticker",
        "ltp": ltp,
        "ltt": ltt
    }

# -------------------------------------------------------------------
# OI ONLY PACKET (Code = 5)
# -------------------------------------------------------------------
def parse_oi_packet(message):
    oi, = struct.unpack(">I", message[8:12])
    return {
        "timestamp": datetime.now().isoformat(),
        "type": "oi",
        "oi": oi
    }

# -------------------------------------------------------------------
# MASTER PACKET ROUTER
# -------------------------------------------------------------------
def handle_packet(message):
    if len(message) < 8:
        return None, None

    code, msg_len, segment, security_id = struct.unpack(">BHB I", message[:8])
    sid = str(security_id)

    if code == 8:
        data = parse_full_packet(message)
    elif code == 4:
        data = parse_quote_packet(message)
    elif code == 2:
        data = parse_ticker_packet(message)
    elif code == 5:
        data = parse_oi_packet(message)
    else:
        return None, None

    live_data[sid] = data
    orderflow_history.setdefault(sid, []).append(data)

    return sid, data

# -------------------------------------------------------------------
# CUSTOM WEBSOCKET CLIENT (no Dhan SDK)
# -------------------------------------------------------------------
class DhanWebSocketClient:
    def __init__(self, access_token, client_id, instruments):
        self.ws_url = (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={access_token}&clientId={client_id}&authType=2"
        )
        self.instruments = instruments

    def on_message(self, ws, message):
        sid, parsed = handle_packet(message)
        if parsed:
            print(f"[WS] {sid} | LTP={parsed.get('ltp')} | OI={parsed.get('oi')}")

    def on_error(self, ws, error):
        print("[ERROR]", error)

    def on_close(self, ws, c, m):
        print("[WS CLOSED]")

    def on_open(self, ws):
        print("[WS OPEN] Subscribing...")
        chunks = [self.instruments[i:i+100] for i in range(0, len(self.instruments), 100)]
        for batch in chunks:
            sub = {"RequestCode": 17, "InstrumentCount": len(batch), "InstrumentList": batch}
            ws.send(json.dumps(sub))

    def run(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        thread = threading.Thread(target=ws.run_forever)
        thread.daemon = True
        thread.start()

# -------------------------------------------------------------------
# API ROUTES
# -------------------------------------------------------------------
@app.route("/api/orderflow/<security_id>")
def get_live(security_id):
    return jsonify(live_data.get(security_id, {}))

@app.route("/api/history/<security_id>")
def get_hist(security_id):
    return jsonify(orderflow_history.get(security_id, []))

# -------------------------------------------------------------------
# MAIN RUN
# -------------------------------------------------------------------
if __name__ == "__main__":
    ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1NTE1NDY2LCJpYXQiOjE3NjU0MjkwNjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwMjQ0MjY4In0.wD3K7JjlDlT28UaHh_hWFtncemKKHIqKekSlfJrDR3h9gDdCTl3vbUADcVfeMqgNwL3JEgOCC9vFXee0CDdkOQ"
    CLIENT_ID = "1100244268"

    # Example instrument list → replace with your CSV loader
    instruments = [
        (2, 12345, 8),   # NSE_FNO, securityId, Full packet = 8
        (2, 67890, 8)
    ]

    ws_client = DhanWebSocketClient(ACCESS_TOKEN, CLIENT_ID, instruments)
    ws_client.run()

    print("Flask server running on port 5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
