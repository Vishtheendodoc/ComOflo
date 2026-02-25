from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import logging
from datetime import datetime
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd
import pytz
IST = pytz.timezone("Asia/Kolkata")

# --- CONFIG ---
API_BATCH_SIZE = 5          # Number of stocks per batch API call
BATCH_INTERVAL_SEC = 5      # Wait time between batches
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# --- LOGGING SETUP ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable debug logging in production
if ENVIRONMENT == 'production':
    logging.disable(logging.DEBUG)

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)

# Set Flask debug mode based on environment
app.config['DEBUG'] = ENVIRONMENT != 'production'

# Global variables
analyzer = None
delta_history = defaultdict(lambda: defaultdict(lambda: {'buy': 0, 'sell': 0}))
last_reset_date = [None]  # For daily reset

# Track previous LTP for tick-rule logic
prev_ltp = defaultdict(lambda: None)

prev_volume = defaultdict(lambda: 0)  # Track previous total volume


# Performance monitoring
stats = {
    'messages_processed': 0,
    'api_calls': 0,
    'errors': 0,
    'db_writes': 0,
    'start_time': datetime.now(IST).isoformat()
}

# Batch processing for database writes
db_batch = []
DB_BATCH_SIZE = 100
db_batch_lock = threading.Lock()

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcyMDgyNzQ2LCJpYXQiOjE3NzE5OTYzNDYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwMjQ0MjY4In0.vedAoU4mMiESQSlot2bnU86jnttfsZ0ybnqwJzQWQKymT2yzTP8yT-xUl0LkKXQWk_kjPaOH5VWJf8NTzCPPTQ"
analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)

# Prepare instrument list from your stock_list.csv
def get_instrument_list():
    stocks = []
    try:
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
        logger.info(f"Loaded {len(stocks)} instruments from {STOCK_LIST_FILE}")
    except Exception as e:
        logger.error(f"Failed to load instrument list: {e}")
        stats['errors'] += 1
    return stocks

instrument_list = get_instrument_list()

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
market_feed = MarketFeed(dhan_context, instrument_list, "v2")

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
            # Create index for faster queries
            conn.execute('CREATE INDEX IF NOT EXISTS idx_security_timestamp ON orderflow(security_id, timestamp)')
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        stats['errors'] += 1

init_db()

def store_in_db_batch(batch_data):
    """Store multiple records in database at once"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.executemany(
                "INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume, buy_initiated, sell_initiated, tick_delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_data
            )
        stats['db_writes'] += len(batch_data)
        logger.debug(f"Batch inserted {len(batch_data)} records")
    except Exception as e:
        logger.error(f"Database batch insert failed: {e}")
        stats['errors'] += 1

def add_to_db_batch(security_id, timestamp, buy, sell, ltp, volume, buy_initiated=None, sell_initiated=None, tick_delta=None):
    """Add record to batch for later database insertion"""
    global db_batch
    
    with db_batch_lock:
        db_batch.append((security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta))
        
        # Write batch when it reaches the threshold
        if len(db_batch) >= DB_BATCH_SIZE:
            store_in_db_batch(db_batch.copy())
            db_batch.clear()

def flush_db_batch():
    """Force write remaining batch data"""
    global db_batch
    
    with db_batch_lock:
        if db_batch:
            store_in_db_batch(db_batch.copy())
            db_batch.clear()

def marketfeed_thread():
    global stats
    
    while True:
        try:
            market_feed.run_forever()
            # Continuously fetch and update live_market_data and orderflow_history
            while True:
                response = market_feed.get_data()
                
                # Removed heavy logging - only log in debug mode
                logger.debug(f"Received market feed response for security: {response.get('security_id') if response else 'None'}")

                if response and isinstance(response, dict):
                    required_keys = ["security_id"]
                    missing_keys = [key for key in required_keys if key not in response]
                    
                    if missing_keys:
                        logger.error(f"Missing keys {missing_keys} in response")
                        stats['errors'] += 1
                        continue
                    
                    security_id = str(response.get("security_id"))
                    if not security_id:
                        logger.warning("Empty security_id in response")
                        stats['errors'] += 1
                        continue

                    # Update stats
                    stats['messages_processed'] += 1
                    
                    # Periodic logging instead of per-message logging
                    if stats['messages_processed'] % 1000 == 0:
                        logger.info(f"Processed {stats['messages_processed']} messages, {stats['errors']} errors, {stats['db_writes']} DB writes")

                    # Update live data
                    live_market_data[security_id] = response
                    
                    # Update history
                    if security_id not in orderflow_history:
                        orderflow_history[security_id] = []
                    orderflow_history[security_id].append(response)
                    
                    # Keep history size manageable
                    if len(orderflow_history[security_id]) > 10000:  # Keep last 10k records
                        orderflow_history[security_id] = orderflow_history[security_id][-5000:]  # Trim to 5k
                    
                    # Store Quote Data in DB (batched)
                    msg_type = response.get("type", "")
                    if msg_type in ("Quote Data", "Quote"):   # ✅ accept both

                        try:
                            ltt = response.get("LTT")
                            today = datetime.now(IST).strftime('%Y-%m-%d')
                            timestamp = f"{today} {ltt}" if ltt else datetime.now(IST).isoformat()
                            buy = response.get("total_buy_quantity", 0)
                            sell = response.get("total_sell_quantity", 0)
                            ltp_val = float(response.get("LTP", 0))
                            volume = response.get("volume", 0)
                            ltq = response.get("LTQ", 0)
                            
                            # Tick-rule logic
                            # 🚀 GoCharting-Style Tick-Rule Logic with Volume De-Duplication
                            prev = prev_ltp[security_id]
                            prev_total_volume = prev_volume.get(security_id, 0)
                            current_total_volume = response.get("volume", 0)
                            delta_volume = current_total_volume - prev_total_volume
                            prev_volume[security_id] = current_total_volume

                            buy_initiated = 0
                            sell_initiated = 0

                            if prev is not None and delta_volume > 0:
                                if ltp_val > prev:
                                    # Price uptick → Buyer aggression
                                    buy_initiated = delta_volume
                                elif ltp_val < prev:
                                    # Price downtick → Seller aggression
                                    sell_initiated = delta_volume
                                else:
                                    # Price flat → use Bid/Ask proximity
                                    bid_price = response.get("bid_price", 0)
                                    ask_price = response.get("ask_price", 0)

                                    if bid_price and ask_price:
                                        if ltp_val >= ask_price:
                                            buy_initiated = delta_volume
                                        elif ltp_val <= bid_price:
                                            sell_initiated = delta_volume
                                        else:
                                            # Trade between bid/ask → split volume
                                            buy_initiated = delta_volume / 2
                                            sell_initiated = delta_volume / 2
                                    else:
                                        # No bid/ask → split
                                        buy_initiated = delta_volume / 2
                                        sell_initiated = delta_volume / 2
                            else:
                                # No new volume → skip
                                buy_initiated = 0
                                sell_initiated = 0

                            tick_delta = buy_initiated - sell_initiated
                            prev_ltp[security_id] = ltp_val

                            
                            # Add to batch instead of direct DB write
                            add_to_db_batch(security_id, timestamp, buy, sell, ltp_val, volume, buy_initiated, sell_initiated, tick_delta)
                            
                        except Exception as e:
                            logger.error(f"Error processing quote data for {security_id}: {e}")
                            stats['errors'] += 1
                
                time.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Marketfeed thread crashed: {e}. Restarting in 5 seconds...")
            stats['errors'] += 1
            time.sleep(5)  # Increased restart delay

# Start the market feed in a background thread
marketfeed_thread_obj = threading.Thread(target=marketfeed_thread, daemon=True)
marketfeed_thread_obj.start()

# Periodic batch flush thread
def batch_flush_thread():
    while True:
        time.sleep(30)  # Flush every 30 seconds
        flush_db_batch()

threading.Thread(target=batch_flush_thread, daemon=True).start()

def maybe_reset_history():
    now = datetime.now(IST)
    today_str = now.strftime('%Y-%m-%d')

    if last_reset_date[0] != today_str and now.strftime('%H:%M') >= RESET_TIME:
        delta_history.clear()
        last_reset_date[0] = today_str
        logger.info(f"Cleared delta history at {now.strftime('%H:%M:%S')}")

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

@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    try:
        interval = int(request.args.get('interval', 5))
    except (ValueError, TypeError):
        interval = 5
    
    try:
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
                logger.error(f"Timestamp parsing failed for security {security_id}")
                return jsonify([])

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
                threshold = 0
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

        # Convert timestamp if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

        # Optional: filter for today only
        today = datetime.now(IST).date()
        df = df[df['timestamp'].dt.date == today]

        # Optional: resample by interval (if needed)
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        tick_deltas = [group['tick_delta'].sum() for _, group in grouped if not group.empty]

        # Calculate cumulative tick delta
        cumulative_tick_delta = sum(tick_deltas)

        return jsonify({
            "security_id": security_id,
            "interval": interval,
            "cumulative_tick_delta": cumulative_tick_delta
        })

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
    
    # Removed heavy logging - only log in debug mode or when there's no data
    if not data:
        logger.debug(f"No live data available for security_id: {security_id}")
        return jsonify({"error": "No live data"}), 404
    
    return jsonify(data)

@app.route('/api/orderflow_history/<string:security_id>')
def get_orderflow_history(security_id):
    stats['api_calls'] += 1
    data = orderflow_history.get(security_id, [])
    return jsonify(data)

@app.route('/api/stats')
def get_stats():
    """Monitoring endpoint for system health"""
    current_stats = stats.copy()
    current_stats['uptime_seconds'] = (datetime.now(IST) - datetime.fromisoformat(stats['start_time'])).total_seconds()
    current_stats['db_batch_size'] = len(db_batch)
    current_stats['live_securities'] = len(live_market_data)
    return jsonify(current_stats)

@app.route('/api/health')
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(IST).isoformat(),
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

# Graceful shutdown
import atexit

def cleanup():
    logger.info("Shutting down gracefully...")
    flush_db_batch()  # Ensure all data is written before shutdown

atexit.register(cleanup)

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=5000)
