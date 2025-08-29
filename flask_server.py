from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import logging
from datetime import datetime
import asyncio
import nest_asyncio
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd
import signal
import sys

# Enable nested event loops for async compatibility
nest_asyncio.apply()

# --- CONFIG ---
API_BATCH_SIZE = 5          # Number of stocks per batch API call
BATCH_INTERVAL_SEC = 30     # Increased interval to avoid rate limits
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"
MAX_RECONNECT_ATTEMPTS = 3  # Limit reconnection attempts
RECONNECT_BACKOFF = [30, 60, 120]  # Progressive backoff in seconds

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

# Market feed state management
market_feed_active = False
market_feed_task = None
reconnect_attempts = 0
last_connection_time = 0

# Performance monitoring
stats = {
    'messages_processed': 0,
    'api_calls': 0,
    'errors': 0,
    'db_writes': 0,
    'reconnect_attempts': 0,
    'last_successful_connection': None,
    'connection_status': 'disconnected',
    'start_time': datetime.now().isoformat()
}

# Batch processing for database writes
db_batch = []
DB_BATCH_SIZE = 50  # Reduced for more frequent writes
db_batch_lock = threading.Lock()

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU4OTQ5MjcwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.xjTrEBxvCxX8kf6P3ZrfvJupkdNBNhL9A2Tf9jrz4UkQ52rz2Jzqj9kX1POdYULPuJp6dFYQ68TL3kQWZImvAg"

try:
    analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
except Exception as e:
    logger.error(f"Failed to initialize OrderFlowAnalyzer: {e}")
    analyzer = None

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
        # Return a minimal list to prevent crashes
        return [(MarketFeed.NSE_FNO, "26000", MarketFeed.Quote)]  # NIFTY 50 as fallback
    return stocks

instrument_list = get_instrument_list()

# Initialize market feed context
dhan_context = None
market_feed = None

def init_market_feed():
    global dhan_context, market_feed
    try:
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        market_feed = MarketFeed(dhan_context, instrument_list, "v2")
        logger.info("Market feed initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize market feed: {e}")
        return False

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

def process_market_data(response):
    """Process individual market data response"""
    global stats
    
    if not response or not isinstance(response, dict):
        return
        
    required_keys = ["security_id"]
    missing_keys = [key for key in required_keys if key not in response]
    
    if missing_keys:
        logger.error(f"Missing keys {missing_keys} in response")
        stats['errors'] += 1
        return
    
    security_id = str(response.get("security_id"))
    if not security_id:
        logger.warning("Empty security_id in response")
        stats['errors'] += 1
        return

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
            
            # Tick-rule logic with volume de-duplication
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

            tick_delta = buy_initiated - sell_initiated
            prev_ltp[security_id] = ltp_val
            
            # Add to batch instead of direct DB write
            add_to_db_batch(security_id, timestamp, buy, sell, ltp_val, volume, buy_initiated, sell_initiated, tick_delta)
            
        except Exception as e:
            logger.error(f"Error processing quote data for {security_id}: {e}")
            stats['errors'] += 1

async def async_market_feed_handler():
    """Async handler for market feed with proper error handling and backoff"""
    global market_feed_active, reconnect_attempts, stats
    
    while market_feed_active:
        try:
            if not market_feed:
                if not init_market_feed():
                    await asyncio.sleep(RECONNECT_BACKOFF[min(reconnect_attempts, len(RECONNECT_BACKOFF)-1)])
                    reconnect_attempts += 1
                    continue
            
            logger.info("Starting market feed connection...")
            stats['connection_status'] = 'connecting'
            
            # Start the market feed
            await market_feed.run_forever()
            
            stats['connection_status'] = 'connected'
            stats['last_successful_connection'] = datetime.now().isoformat()
            reconnect_attempts = 0  # Reset on successful connection
            
            # Process messages
            while market_feed_active:
                try:
                    response = await market_feed.get_data()
                    if response:
                        process_market_data(response)
                    await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
                except asyncio.CancelledError:
                    logger.info("Market feed task cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error processing market data: {e}")
                    stats['errors'] += 1
                    await asyncio.sleep(1)  # Brief pause on error
                    
        except Exception as e:
            error_msg = str(e).lower()
            stats['connection_status'] = 'disconnected'
            stats['errors'] += 1
            stats['reconnect_attempts'] += 1
            
            if "429" in error_msg or "rate limit" in error_msg:
                # Rate limiting - use exponential backoff
                backoff_time = RECONNECT_BACKOFF[min(reconnect_attempts, len(RECONNECT_BACKOFF)-1)]
                logger.warning(f"Rate limited. Waiting {backoff_time}s before reconnecting...")
                await asyncio.sleep(backoff_time)
                reconnect_attempts += 1
                
                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    logger.error("Max reconnection attempts reached. Stopping market feed.")
                    market_feed_active = False
                    break
                    
            elif "event loop" in error_msg:
                logger.error("Event loop conflict detected. This may require code restructuring.")
                await asyncio.sleep(30)
                
            else:
                logger.error(f"Market feed error: {e}. Reconnecting in 10 seconds...")
                await asyncio.sleep(10)
        
        # Brief pause between connection attempts
        if market_feed_active:
            await asyncio.sleep(5)

def start_market_feed_thread():
    """Start the market feed in a separate thread with async event loop"""
    global market_feed_active, market_feed_task
    
    def run_async_loop():
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            market_feed_task = loop.create_task(async_market_feed_handler())
            loop.run_until_complete(market_feed_task)
            
        except Exception as e:
            logger.error(f"Event loop error: {e}")
        finally:
            try:
                loop.close()
            except:
                pass
    
    market_feed_active = True
    thread = threading.Thread(target=run_async_loop, daemon=True)
    thread.start()
    logger.info("Market feed thread started")

# Periodic batch flush thread
def batch_flush_thread():
    while True:
        time.sleep(20)  # Flush every 20 seconds
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
                buy_initiated_sum = group['buy_initiated'].sum() if 'buy_initiated' in group.columns else 0
                sell_initiated_sum = group['sell_initiated'].sum() if 'sell_initiated' in group.columns else 0
                tick_delta_sum = group['tick_delta'].sum() if 'tick_delta' in group.columns else 0
                
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
    current_stats['market_feed_active'] = market_feed_active
    return jsonify(current_stats)

@app.route('/api/health')
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({
        "status": "healthy" if market_feed_active else "degraded",
        "timestamp": datetime.now().isoformat(),
        "securities_count": len(live_market_data),
        "connection_status": stats.get('connection_status', 'unknown')
    })

@app.route('/api/restart_feed')
def restart_market_feed():
    """Manual endpoint to restart market feed"""
    global market_feed_active, reconnect_attempts
    
    market_feed_active = False
    time.sleep(2)  # Allow current operations to finish
    
    reconnect_attempts = 0
    start_market_feed_thread()
    
    return jsonify({
        "status": "restarted",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/')
def dashboard():
    return f"""
    <h1>Order Flow Flask Server</h1>
    <p><strong>Environment:</strong> {ENVIRONMENT}</p>
    <p><strong>Log Level:</strong> {LOG_LEVEL}</p>
    <p><strong>Connection Status:</strong> {stats.get('connection_status', 'unknown')}</p>
    <p><strong>Market Feed Active:</strong> {market_feed_active}</p>
    <p><strong>Processed Messages:</strong> {stats['messages_processed']}</p>
    <p><strong>API Calls:</strong> {stats['api_calls']}</p>
    <p><strong>Errors:</strong> {stats['errors']}</p>
    <p><strong>DB Writes:</strong> {stats['db_writes']}</p>
    <p><strong>Reconnection Attempts:</strong> {stats['reconnect_attempts']}</p>
    <p><strong>Last Connection:</strong> {stats.get('last_successful_connection', 'Never')}</p>
    <hr>
    <p>
        <a href="/api/stats">Stats</a> | 
        <a href="/api/health">Health</a> | 
        <a href="/api/restart_feed">Restart Feed</a>
    </p>
    """

# Graceful shutdown
import atexit

def cleanup():
    global market_feed_active
    logger.info("Shutting down gracefully...")
    market_feed_active = False
    flush_db_batch()  # Ensure all data is written before shutdown
    
    if market_feed_task and not market_feed_task.done():
        market_feed_task.cancel()

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down...")
    cleanup()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
atexit.register(cleanup)

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    
    # Initialize market feed after a short delay to ensure Flask is ready
    def delayed_start():
        time.sleep(2)
        start_market_feed_thread()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    
    # Run Flask app
    app.run(
        debug=app.config['DEBUG'], 
        host='0.0.0.0', 
        port=5000,
        use_reloader=False  # Disable reloader to prevent duplicate threads
    )