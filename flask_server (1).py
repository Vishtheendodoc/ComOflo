from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
import time
from datetime import datetime
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd

# --- CONFIG ---
API_BATCH_SIZE = 5          # Number of stocks per batch API call
BATCH_INTERVAL_SEC = 5      # Wait time between batches
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)

# Global variables
analyzer = None
delta_history = defaultdict(lambda: defaultdict(lambda: {'buy': 0, 'sell': 0}))
last_reset_date = [None]  # For daily reset

# Track previous LTP for tick-rule logic
prev_ltp = defaultdict(lambda: None)

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzUzODUzOTQxLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.DsXutZuv9qIh4MNpMrfjIccyWg_-nwR9t5ldK0H14aGY23U7IaFW6cTmXp1MAr8zjk7eAIYNe-SVLv19Skw4MQ"
analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)

# Prepare instrument list from your stock_list.csv
def get_instrument_list():
    stocks = []
    with open(STOCK_LIST_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            exch = row.get("exchange")
            seg = row.get("segment")
            instr = row.get("instrument")
            sec_id = str(row["security_id"])
            # Map to MarketFeed enums/constants
            if exch == "NSE" and seg == "D":
                stocks.append((MarketFeed.NSE_FNO, sec_id, MarketFeed.Quote))
            elif exch == "MCX" and seg == "M":
                stocks.append(("MCX_COMM", sec_id, MarketFeed.Quote))  # Use string for MCX
    return stocks

instrument_list = get_instrument_list()

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
market_feed = MarketFeed(dhan_context, instrument_list, "v2")

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS orderflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                security_id TEXT,
                timestamp TEXT,
                buy_volume REAL,
                sell_volume REAL,
                ltp REAL,
                volume REAL,
                buy_initiated REAL,
                sell_initiated REAL,
                tick_delta REAL
            )
        ''')
init_db()

def store_in_db(security_id, timestamp, buy, sell, ltp, volume, buy_initiated=None, sell_initiated=None, tick_delta=None):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume, buy_initiated, sell_initiated, tick_delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta)
        )

def marketfeed_thread():
    while True:
        try:
            market_feed.run_forever()
            # Continuously fetch and update live_market_data and orderflow_history
            while True:
                response = market_feed.get_data()
                print("Raw response from market feed:", response)  # Log the raw data

                if response and isinstance(response, dict):
                    required_keys = ["security_id"]  # Add more keys as needed
                    for key in required_keys:
                        if key not in response:
                            print(f"Error: Missing key '{key}' in response: {response}")
                    security_id = str(response.get("security_id"))
                    if not security_id:
                        print("Warning: security_id missing in response:", response)
                    else:
                        print(f"Parsed security_id: {security_id}, data: {response}")  # Log parsed data

                    if security_id:
                        live_market_data[security_id] = response
                        if security_id not in orderflow_history:
                            orderflow_history[security_id] = []
                        orderflow_history[security_id].append(response)
                        # Store Quote Data in DB
                        if response.get('type') == 'Quote Data':
                            ltt = response.get("LTT")
                            today = datetime.now().strftime('%Y-%m-%d')
                            timestamp = f"{today} {ltt}" if ltt else datetime.now().isoformat()
                            buy = response.get("total_buy_quantity", 0)
                            sell = response.get("total_sell_quantity", 0)
                            ltp = response.get("LTP", 0)
                            volume = response.get("volume", 0)
                            ltq = response.get("LTQ", 0)
                            # Tick-rule logic
                            prev = prev_ltp[security_id]
                            buy_initiated = 0
                            sell_initiated = 0
                            if prev is not None and ltq:
                                if ltp > prev:
                                    buy_initiated = ltq
                                elif ltp < prev:
                                    sell_initiated = ltq
                                # else: unchanged, both 0
                            tick_delta = buy_initiated - sell_initiated
                            prev_ltp[security_id] = ltp
                            store_in_db(security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta)
                time.sleep(0.01)
        except Exception as e:
            print(f"[ERROR] Marketfeed thread crashed: {e}. Restarting thread...")
            time.sleep(2)

# Start the market feed in a background thread
threading.Thread(target=marketfeed_thread, daemon=True).start()

def maybe_reset_history():
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')

    if last_reset_date[0] != today_str and now.strftime('%H:%M') >= RESET_TIME:
        delta_history.clear()
        last_reset_date[0] = today_str
        print(f"🧹 Cleared delta history at {now.strftime('%H:%M:%S')}")


def load_stock_list():
    stocks = []
    with open(STOCK_LIST_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            stocks.append((row["security_id"], row["symbol"]))
    print(f"📃 Loaded {len(stocks)} stocks from {STOCK_LIST_FILE}")
    return stocks


@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    try:
        interval = int(request.args.get('interval', 5))
    except Exception:
        interval = 5
    # Query from SQLite DB
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM orderflow WHERE security_id = ? ORDER BY timestamp ASC",
            conn, params=(security_id,)
        )
    if df.empty:
        return jsonify([])
    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['timestamp'])
        except Exception:
            pass
    df['minute'] = df['timestamp'].dt.strftime('%H:%M')
    buckets = []
    grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
    for bucket_label, group in grouped:
        if len(group) >= 2:
            start = group.iloc[0]
            end = group.iloc[-1]
            buy_delta = end['buy_volume'] - start['buy_volume']
            sell_delta = end['sell_volume'] - start['sell_volume']
            delta = buy_delta - sell_delta
            # Tick-rule aggregation
            buy_initiated_sum = group['buy_initiated'].sum()
            sell_initiated_sum = group['sell_initiated'].sum()
            tick_delta_sum = group['tick_delta'].sum()
            # Inference
            threshold = 0  # You can adjust this if needed
            if tick_delta_sum > threshold:
                inference = "Buy Dominant"
            elif tick_delta_sum < -threshold:
                inference = "Sell Dominant"
            else:
                inference = "Neutral"
            # OHLC from LTP
            ohlc = group['ltp'].dropna()
            if not ohlc.empty:
                open_ = ohlc.iloc[0]
                high_ = ohlc.max()
                low_ = ohlc.min()
                close_ = ohlc.iloc[-1]
            else:
                open_ = high_ = low_ = close_ = None
            buckets.append({
                'timestamp': bucket_label.strftime('%H:%M'),
                'buy_volume': buy_delta,
                'sell_volume': sell_delta,
                'delta': delta,
                'buy_initiated': buy_initiated_sum,
                'sell_initiated': sell_initiated_sum,
                'tick_delta': tick_delta_sum,
                'inference': inference,
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_
            })
        elif len(group) == 1:
            row = group.iloc[0]
            ohlc = row['ltp'] if pd.notnull(row['ltp']) else None
            buckets.append({
                'timestamp': bucket_label.strftime('%H:%M'),
                'buy_volume': 0,
                'sell_volume': 0,
                'delta': 0,
                'buy_initiated': 0,
                'sell_initiated': 0,
                'tick_delta': 0,
                'inference': 'Neutral',
                'open': ohlc,
                'high': ohlc,
                'low': ohlc,
                'close': ohlc
            })
    return jsonify(buckets)


@app.route('/api/stocks')
def get_stock_list():
    try:
        stocks = load_stock_list()
        return jsonify([{"security_id": sid, "symbol": sym} for sid, sym in stocks])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/live_data/<string:security_id>')
def get_live_data(security_id):
    data = live_market_data.get(security_id)
    print(f"API /api/live_data/{security_id} response: {data}")  # Log API output
    if data:
        return jsonify(data)
    else:
        return jsonify({"error": "No live data"}), 404


@app.route('/api/orderflow_history/<string:security_id>')
def get_orderflow_history(security_id):
    data = orderflow_history.get(security_id, [])
    return jsonify(data)


@app.route('/')
def dashboard():
    return "<h1>Order Flow Flask Server Running</h1>"


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
