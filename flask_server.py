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
import time
from collections import deque

# --- ENHANCED CONFIG FOR RATE LIMITING ---
API_BATCH_SIZE = 3          # Reduced from 5 to 3
BATCH_INTERVAL_SEC = 10     # Increased from 5 to 10 seconds
RESET_TIME = "09:15"        
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# Rate limiting configuration
MAX_REQUESTS_PER_MINUTE = 50  # Adjust based on your API limits
REQUEST_DELAY = 60 / MAX_REQUESTS_PER_MINUTE  # Delay between requests

# Track request timestamps for rate limiting
request_timestamps = deque()

def rate_limit_check():
    """Check if we should delay the request to respect rate limits"""
    now = time.time()
    
    # Remove timestamps older than 1 minute
    while request_timestamps and now - request_timestamps[0] > 60:
        request_timestamps.popleft()
    
    # If we're at the limit, wait
    if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
        sleep_time = 60 - (now - request_timestamps[0]) + 1
        if sleep_time > 0:
            logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
    
    # Add current request timestamp
    request_timestamps.append(now)

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
    'start_time': datetime.now().isoformat()
}

# Batch processing for database writes
db_batch = []
DB_BATCH_SIZE = 100
db_batch_lock = threading.Lock()

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2Mjk2NDY0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.OeP6vi2JlsYTNM8QkW-otqCiN0RnWOdddrJ-1rsFUUo7SBGx_emR__zzBi67vjRvQ7I2Pl2GtCaJvkh13h0DOw"
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
                
                # Rate limit check before adding each instrument
                rate_limit_check()
                
                # Map to MarketFeed enums/constants
                if exch == "NSE" and seg == "D":
                    stocks.append((MarketFeed.NSE_FNO, sec_id, MarketFeed.Quote))
                elif exch == "MCX" and seg == "M":
                    stocks.append(("MCX_COMM", sec_id, MarketFeed.Quote))
                    
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
    consecutive_errors = 0
    max_consecutive_errors = 5
    base_delay = 1  # Base delay in seconds
    max_delay = 60  # Maximum delay in seconds
    
    while True:
        try:
            market_feed.run_forever()
            
            while True:
                response = market_feed.get_data()
                
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

                    # Reset consecutive errors on successful response
                    consecutive_errors = 0
                    
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
                    if len(orderflow_history[security_id]) > 10000:
                        orderflow_history[security_id] = orderflow_history[security_id][-5000:]
                    
                    # Store Quote Data in DB (batched)
                    if response.get('type') == 'Quote Data':
                        try:
                            ltt = response.get("LTT")
                            today = datetime.now().strftime('%Y-%m-%d')
                            timestamp = f"{today} {ltt}" if ltt else datetime.now().isoformat()
                            buy = response.get("total_buy_quantity", 0)
                            sell = response.get("total_sell_quantity", 0)
                            ltp_val = float(response.get("LTP", 0))
                            volume = response.get("volume", 0)
                            ltq = response.get("LTQ", 0)
                            
                            # Tick-rule logic
                            prev = prev_ltp[security_id]
                            prev_total_volume = prev_volume.get(security_id, 0)
                            current_total_volume = response.get("volume", 0)
                            delta_volume = current_total_volume - prev_total_volume
                            prev_volume[security_id] = current_total_volume

                            buy_initiated = 0
                            sell_initiated = 0

                            if prev is not None and delta_volume > 0:
                                if ltp_val > prev:
                                    buy_initiated = delta_volume
                                elif ltp_val < prev:
                                    sell_initiated = delta_volume
                                else:
                                    bid_price = response.get("bid_price", 0)
                                    ask_price = response.get("ask_price", 0)

                                    if bid_price and ask_price:
                                        if ltp_val >= ask_price:
                                            buy_initiated = delta_volume
                                        elif ltp_val <= bid_price:
                                            sell_initiated = delta_volume
                                        else:
                                            buy_initiated = delta_volume / 2
                                            sell_initiated = delta_volume / 2
                                    else:
                                        buy_initiated = delta_volume / 2
                                        sell_initiated = delta_volume / 2
                            else:
                                buy_initiated = 0
                                sell_initiated = 0

                            tick_delta = buy_initiated - sell_initiated
                            prev_ltp[security_id] = ltp_val
                            
                            add_to_db_batch(security_id, timestamp, buy, sell, ltp_val, volume, buy_initiated, sell_initiated, tick_delta)
                            
                        except Exception as e:
                            logger.error(f"Error processing quote data for {security_id}: {e}")
                            stats['errors'] += 1
                
                # Add adaptive delay based on API performance
                time.sleep(0.05)  # Increased from 0.01 to reduce API pressure
                
        except Exception as e:
            consecutive_errors += 1
            stats['errors'] += 1
            
            # Check if it's a 429 rate limit error
            if "429" in str(e) or "rate limit" in str(e).lower():
                # Exponential backoff for rate limit errors
                delay = min(base_delay * (2 ** consecutive_errors), max_delay)
                logger.warning(f"Rate limit hit (429). Backing off for {delay} seconds. Error: {e}")
                time.sleep(delay)
            elif consecutive_errors >= max_consecutive_errors:
                # For other errors, use longer delay if too many consecutive errors
                delay = min(base_delay * consecutive_errors, max_delay)
                logger.error(f"Marketfeed thread crashed: {e}. Too many consecutive errors ({consecutive_errors}). Restarting in {delay} seconds...")
                time.sleep(delay)
            else:
                # Short delay for occasional errors
                logger.error(f"Marketfeed thread crashed: {e}. Restarting in 5 seconds...")
                time.sleep(5)

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
    now = datetime.now()
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
        today = datetime.now().date()
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
    current_stats['uptime_seconds'] = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    current_stats['db_batch_size'] = len(db_batch)
    current_stats['live_securities'] = len(live_market_data)
    return jsonify(current_stats)

@app.route('/api/health')
def health_check():
    """Health check endpoint for monitoring"""
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

# Add a health check for rate limiting in your stats endpoint:
@app.route('/api/stats')
def get_stats():
    """Monitoring endpoint for system health"""
    current_stats = stats.copy()
    current_stats['uptime_seconds'] = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    current_stats['db_batch_size'] = len(db_batch)
    current_stats['live_securities'] = len(live_market_data)
    current_stats['requests_last_minute'] = len(request_timestamps)
    current_stats['rate_limit_active'] = len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE * 0.8  # 80% threshold
    return jsonify(current_stats)

# Graceful shutdown
import atexit

def cleanup():
    logger.info("Shutting down gracefully...")
    flush_db_batch()  # Ensure all data is written before shutdown

atexit.register(cleanup)

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=5000)
