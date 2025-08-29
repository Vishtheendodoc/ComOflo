from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import logging
import random
from datetime import datetime
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd

# --- CONFIG ---
API_BATCH_SIZE = 5
BATCH_INTERVAL_SEC = 5
RESET_TIME = "09:15"
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# Pull creds from env in prod (fallback to literals for dev)
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1100244268")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2Mjk2NDY0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.OeP6vi2JlsYTNM8QkW-otqCiN0RnWOdddrJ-1rsFUUo7SBGx_emR__zzBi67vjRvQ7I2Pl2GtCaJvkh13h0DOw"
)

# --- LOGGING ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
if ENVIRONMENT == 'production':
    logging.disable(logging.DEBUG)

# --- FLASK ---
app = Flask(__name__)
CORS(app)
app.config['DEBUG'] = ENVIRONMENT != 'production'

# --- STATE ---
analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
delta_history = defaultdict(lambda: defaultdict(lambda: {'buy': 0, 'sell': 0}))
last_reset_date = [None]
prev_ltp = defaultdict(lambda: None)
prev_volume = defaultdict(lambda: 0)

stats = {
    'messages_processed': 0,
    'api_calls': 0,
    'errors': 0,
    'db_writes': 0,
    'start_time': datetime.now().isoformat()
}

db_batch = []
DB_BATCH_SIZE = 100
db_batch_lock = threading.Lock()

# --- DHAN CONTEXT + FEED (instantiate once, subscribe after connect) ---
dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
# create with empty instruments; we'll subscribe after WS connection
market_feed = MarketFeed(dhan_context, [], "v2")

# --- DB ---
def init_db():
    try:
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
            conn.execute('CREATE INDEX IF NOT EXISTS idx_security_timestamp ON orderflow(security_id, timestamp)')
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        stats['errors'] += 1

init_db()

def store_in_db_batch(batch_data):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.executemany(
                "INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume, buy_initiated, sell_initiated, tick_delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_data
            )
        stats['db_writes'] += len(batch_data)
        logger.debug(f"Batch inserted {len(batch_data)} records")
    except Exception as e:
        logger.error(f"DB batch insert failed: {e}")
        stats['errors'] += 1

def add_to_db_batch(security_id, timestamp, buy, sell, ltp, volume, buy_initiated=None, sell_initiated=None, tick_delta=None):
    global db_batch
    with db_batch_lock:
        db_batch.append((security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta))
        if len(db_batch) >= DB_BATCH_SIZE:
            store_in_db_batch(db_batch.copy())
            db_batch.clear()

def flush_db_batch():
    global db_batch
    with db_batch_lock:
        if db_batch:
            store_in_db_batch(db_batch.copy())
            db_batch.clear()

# --- STOCKS / INSTRUMENTS ---
def get_instrument_list():
    stocks = []
    try:
        with open(STOCK_LIST_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                exch = row.get("exchange")
                seg = row.get("segment")
                sec_id = str(row["security_id"])
                # NOTE: Use official enums. NSE_FNO is for F&O; for equity cash use NSE_EQUITY (adjust if needed).
                if exch == "NSE" and seg == "D":
                    stocks.append((MarketFeed.NSE_FNO, sec_id, MarketFeed.Quote))
                elif exch == "MCX" and seg == "M":
                    stocks.append((MarketFeed.MCX_COMM, sec_id, MarketFeed.Quote))
        logger.info(f"Loaded {len(stocks)} instruments from {STOCK_LIST_FILE}")
    except Exception as e:
        logger.error(f"Failed to load instrument list: {e}")
        stats['errors'] += 1
    return stocks

# --- SUBSCRIBE (rate-limited) ---
def subscribe_in_batches(feed, instruments, batch_size=100, delay_sec=1.2):
    """
    Dhan Quote WS effective limit: ~1 request/sec. We send one subscribe call per batch.
    """
    for i in range(0, len(instruments), batch_size):
        batch = instruments[i:i + batch_size]
        try:
            feed.subscribe(batch)
            logger.info(f"Subscribed batch {i // batch_size + 1} (size={len(batch)})")
        except Exception as e:
            logger.error(f"Subscription failed for batch {i // batch_size + 1}: {e}")
            # If server hints too many requests, wait longer
            time.sleep(5)
        # Strict throttle: 1 request per second (plus cushion)
        time.sleep(delay_sec)

# --- PROCESS ONE MESSAGE ---
def process_market_message(response):
    if not response or not isinstance(response, dict):
        return

    security_id = str(response.get("security_id") or "")
    if not security_id:
        return

    stats['messages_processed'] += 1
    if stats['messages_processed'] % 2000 == 0:
        logger.info(f"Processed {stats['messages_processed']} messages; errors={stats['errors']}; db_writes={stats['db_writes']}")

    # in-memory mirrors
    live_market_data[security_id] = response
    if security_id not in orderflow_history:
        orderflow_history[security_id] = []
    orderflow_history[security_id].append(response)
    if len(orderflow_history[security_id]) > 10000:
        orderflow_history[security_id] = orderflow_history[security_id][-5000:]

    # persist Quote Data
    if response.get("type") == "Quote Data":
        try:
            ltt = response.get("LTT")
            today = datetime.now().strftime('%Y-%m-%d')
            timestamp = f"{today} {ltt}" if ltt else datetime.now().isoformat()

            buy_qty = response.get("total_buy_quantity", 0)
            sell_qty = response.get("total_sell_quantity", 0)
            ltp_val = float(response.get("LTP", 0) or 0)
            volume = response.get("volume", 0)

            # Tick rule with volume de-dup
            prev = prev_ltp[security_id]
            prev_total_volume = prev_volume.get(security_id, 0)
            current_total_volume = volume
            delta_volume = max(0, current_total_volume - prev_total_volume)  # guard against resets
            prev_volume[security_id] = current_total_volume

            buy_initiated = sell_initiated = 0
            if prev is not None and delta_volume > 0:
                if ltp_val > prev:
                    buy_initiated = delta_volume
                elif ltp_val < prev:
                    sell_initiated = delta_volume
                else:
                    bid_price = response.get("bid_price") or 0
                    ask_price = response.get("ask_price") or 0
                    if bid_price and ask_price:
                        if ltp_val >= ask_price:
                            buy_initiated = delta_volume
                        elif ltp_val <= bid_price:
                            sell_initiated = delta_volume
                        else:
                            buy_initiated = sell_initiated = delta_volume / 2.0
                    else:
                        buy_initiated = sell_initiated = delta_volume / 2.0

            tick_delta = buy_initiated - sell_initiated
            prev_ltp[security_id] = ltp_val

            add_to_db_batch(
                security_id, timestamp,
                buy_qty, sell_qty,
                ltp_val, volume,
                buy_initiated, sell_initiated, tick_delta
            )
        except Exception as e:
            logger.error(f"Error processing quote data for {security_id}: {e}")
            stats['errors'] += 1

# --- MARKETFEED THREAD WITH BACKOFF & ONE-TIME SUBSCRIBE PER CONNECTION ---
def marketfeed_thread():
    instruments = get_instrument_list()
    attempt = 0

    while True:
        try:
            logger.info("Connecting MarketFeed WebSocket...")
            market_feed.run_forever()  # connect (blocking until connected internally)

            # Successful connect -> subscribe in throttled batches
            subscribe_in_batches(market_feed, instruments, batch_size=100, delay_sec=1.2)
            attempt = 0  # reset backoff after successful connect & subscribe
            logger.info("Subscribed to all instruments; starting consume loop")

            while True:
                # Use get_data() as provided by SDK; avoid tight spin if None
                response = market_feed.get_data()
                if response:
                    # Optional: fast-path detect server throttle note
                    msg = str(response)
                    if "429" in msg or "Too Many Requests" in msg:
                        raise RuntimeError("Server replied 429 within data stream")
                    process_market_message(response)
                else:
                    # small sleep to avoid busy-wait when feed is quiet
                    time.sleep(0.005)

        except Exception as e:
            stats['errors'] += 1
            # Heavier backoff if it looks like 429 / throttling
            txt = str(e)
            throttled = any(s in txt for s in ("429", "Too Many Requests", "rate", "Rate"))
            base = min(60, (2 ** attempt))  # 1,2,4,8,...,60
            if throttled:
                base = max(base, 15)  # make sure we wait ≥15s when throttled
            delay = base + random.uniform(0, 5)
            logger.error(f"MarketFeed crashed: {e}. Reconnecting in {delay:.1f}s...")
            time.sleep(delay)
            attempt += 1

# --- BATCH FLUSHER ---
def batch_flush_thread():
    while True:
        time.sleep(30)
        flush_db_batch()

threading.Thread(target=batch_flush_thread, daemon=True).start()

# --- SCHEDULED CLEAN ---
def maybe_reset_history():
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    if last_reset_date[0] != today_str and now.strftime('%H:%M') >= RESET_TIME:
        delta_history.clear()
        last_reset_date[0] = today_str
        logger.info(f"Cleared delta history at {now.strftime('%H:%M:%S')}")

# --- STOCKS API ---
def load_stock_list():
    stocks = []
    try:
        with open(STOCK_LIST_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                stocks.append((row["security_id"], row["symbol"]))
        logger.info(f"Loaded {len(stocks)} stocks from {STOCK_LIST_FILE}")
    except Exception as e:
        logger.error(f"Failed to load stock list: {e}")
        stats['errors'] += 1
    return stocks

# --- ROUTES ---
@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    try:
        interval = int(request.args.get('interval', 5))
    except (ValueError, TypeError):
        interval = 5

    try:
        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM orderflow WHERE security_id = ? ORDER BY timestamp ASC",
                conn, params=(security_id,)
            )
        if df.empty:
            return jsonify([])

        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

        buckets = []
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        for bucket_label, group in grouped:
            if group.empty:
                continue
            if len(group) >= 2:
                start, end = group.iloc[0], group.iloc[-1]
                buy_delta = (end['buy_volume'] or 0) - (start['buy_volume'] or 0)
                sell_delta = (end['sell_volume'] or 0) - (start['sell_volume'] or 0)
                delta = buy_delta - sell_delta
                buy_initiated_sum = (group['buy_initiated'].fillna(0)).sum()
                sell_initiated_sum = (group['sell_initiated'].fillna(0)).sum()
                tick_delta_sum = (group['tick_delta'].fillna(0)).sum()

                ohlc = group['ltp'].dropna()
                if not ohlc.empty:
                    open_, high_, low_, close_ = ohlc.iloc[0], ohlc.max(), ohlc.min(), ohlc.iloc[-1]
                else:
                    open_ = high_ = low_ = close_ = None

                inference = "Neutral"
                if tick_delta_sum > 0:
                    inference = "Buy Dominant"
                elif tick_delta_sum < 0:
                    inference = "Sell Dominant"

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
            else:
                row = group.iloc[0]
                ohlc = row['ltp'] if pd.notnull(row['ltp']) else None
                buckets.append({
                    'timestamp': bucket_label.strftime('%H:%M'),
                    'buy_volume': 0, 'sell_volume': 0, 'delta': 0,
                    'buy_initiated': 0, 'sell_initiated': 0, 'tick_delta': 0,
                    'inference': 'Neutral',
                    'open': ohlc, 'high': ohlc, 'low': ohlc, 'close': ohlc
                })
        return jsonify(buckets)

    except Exception as e:
        logger.error(f"Error in get_delta_data for {security_id}: {e}")
        stats['errors'] += 1
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/cumulative_delta/<string:security_id>')
def get_cumulative_tick_delta(security_id):
    try:
        interval = int(request.args.get('interval', 5))
        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql_query(
                "SELECT timestamp, tick_delta FROM orderflow WHERE security_id = ? ORDER BY timestamp ASC",
                conn, params=(security_id,)
            )
        if df.empty or 'tick_delta' not in df.columns:
            return jsonify({"security_id": security_id, "cumulative_tick_delta": 0})

        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

        today = datetime.now().date()
        df = df[df['timestamp'].dt.date == today]
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        cumulative_tick_delta = sum((group['tick_delta'].fillna(0)).sum() for _, group in grouped if not group.empty)

        return jsonify({"security_id": security_id, "interval": interval, "cumulative_tick_delta": cumulative_tick_delta})
    except Exception as e:
        logger.error(f"Error in get_cumulative_tick_delta for {security_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/stocks')
def get_stock_list():
    try:
        stocks = load_stock_list()
        stats['api_calls'] += 1
        return jsonify([{"security_id": sid, "symbol": sym} for sid, sym in stocks])
    except Exception as e:
        logger.error(f"Error in get_stock_list: {e}")
        stats['errors'] += 1
        return jsonify({"error": str(e)}), 500

@app.route('/api/live_data/<string:security_id>')
def get_live_data(security_id):
    stats['api_calls'] += 1
    data = live_market_data.get(security_id)
    if not data:
        logger.debug(f"No live data for security_id: {security_id}")
        return jsonify({"error": "No live data"}), 404
    return jsonify(data)

@app.route('/api/orderflow_history/<string:security_id>')
def get_orderflow_history(security_id):
    stats['api_calls'] += 1
    data = orderflow_history.get(security_id, [])
    return jsonify(data)

@app.route('/api/stats')
def get_stats():
    current_stats = stats.copy()
    current_stats['uptime_seconds'] = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    current_stats['db_batch_size'] = len(db_batch)
    current_stats['live_securities'] = len(live_market_data)
    return jsonify(current_stats)

@app.route('/api/health')
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "securities_count": len(live_market_data)
    })

@app.route('/')
def dashboard():
    return f"""
    <h1>Order Flow Flask Server Running</h1>
    <p>Environment: {ENVIRONMENT}</p>
    <p>Log Level: {LOG_LEVEL}</p>
    <p>Processed Messages: {stats['messages_processed']}</p>
    <p>API Calls: {stats['api_calls']}</p>
    <p>Errors: {stats['errors']}</p>
    <p>DB Writes: {stats['db_writes']}</p>
    <p><a href="/api/stats">Stats</a> | <a href="/api/health">Health</a></p>
    """

# --- GRACEFUL SHUTDOWN ---
import atexit
def cleanup():
    try:
        logger.info("Shutting down gracefully...")
        flush_db_batch()
        try:
            # if SDK exposes close/stop, call it
            if hasattr(market_feed, "close"):
                market_feed.close()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
atexit.register(cleanup)

# --- START BACKGROUND FEED THREAD ---
marketfeed_thread_obj = threading.Thread(target=marketfeed_thread, daemon=True)
marketfeed_thread_obj.start()

# --- MAIN ---
if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=5000)
