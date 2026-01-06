from flask import Flask, render_template, jsonify, request, g
from flask_cors import CORS
import threading
import time
import os
import logging
import signal
import shutil
from datetime import datetime, timedelta, time as dt_time
from functools import lru_cache
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd
import pytz
import json
import hashlib
IST = pytz.timezone("Asia/Kolkata")

# --- LOGGING SETUP (must be early) ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import flask-compress (optional)
try:
    from flask_compress import Compress
    COMPRESS_AVAILABLE = True
except ImportError:
    COMPRESS_AVAILABLE = False
    logger.warning("flask-compress not available, compression disabled")

# --- CONFIG ---
API_BATCH_SIZE = 5          # Number of stocks per batch API call
BATCH_INTERVAL_SEC = 5      # Wait time between batches
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"

# Use persistent disk path on Render (or local for development)
# On Render, use a custom directory (not reserved paths like /opt/render/project/src/)
# Recommended: /data or /app/data for persistent storage
PERSISTENT_DIR = os.getenv('PERSISTENT_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
if not os.path.exists(PERSISTENT_DIR):
    os.makedirs(PERSISTENT_DIR, exist_ok=True)
    logger.info(f"Created persistent directory: {PERSISTENT_DIR}")

DB_FILE = os.path.join(PERSISTENT_DIR, "orderflow_data.db")
BACKUP_DIR = os.path.join(PERSISTENT_DIR, "backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

# Cache configuration (in-memory, can upgrade to Redis later)
CACHE_TTL = 300  # 5 minutes cache TTL
query_cache = {}  # Simple in-memory cache
cache_lock = threading.Lock()

# Disable debug logging in production
if ENVIRONMENT == 'production':
    logging.disable(logging.DEBUG)

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)

# Enable gzip compression (like GoCharting) - optional
if COMPRESS_AVAILABLE:
    Compress(app)
    app.config['COMPRESS_MIMETYPES'] = ['text/html', 'text/css', 'application/json', 'application/javascript']
    app.config['COMPRESS_LEVEL'] = 6
    app.config['COMPRESS_MIN_SIZE'] = 500
    logger.info("✅ Response compression enabled")
else:
    logger.warning("⚠️ Response compression disabled (flask-compress not installed)")

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
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzYzODI1LCJpYXQiOjE3Njc2Nzc0MjUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwMjQ0MjY4In0.FGu9WngrwjRoS4KT3M2oieDCe16bi6Ca093coOYyOJFmGqIamWDYfmu0UFT0rN62FUZm5zmgSZ0FKRNOMTLpVg"
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
    """Initialize database with optimized indexes for GoCharting-like performance"""
    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            # Enable WAL mode for better concurrency (like GoCharting)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')  # Faster writes, still safe
            conn.execute('PRAGMA cache_size=10000')  # 10MB cache
            conn.execute('PRAGMA temp_store=MEMORY')
            
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
            
            # Create optimized indexes for fast queries (like GoCharting)
            conn.execute('CREATE INDEX IF NOT EXISTS idx_security_timestamp ON orderflow(security_id, timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON orderflow(timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_security_id ON orderflow(security_id)')
            
            # Create aggregated data table for faster dashboard loads
            conn.execute('''
                CREATE TABLE IF NOT EXISTS orderflow_aggregated (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    security_id TEXT,
                    date TEXT,
                    interval_minutes INTEGER,
                    timestamp TEXT,
                    buy_volume REAL,
                    sell_volume REAL,
                    ltp REAL,
                    buy_initiated REAL,
                    sell_initiated REAL,
                    tick_delta REAL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    UNIQUE(security_id, date, interval_minutes, timestamp)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_agg_security_date ON orderflow_aggregated(security_id, date, interval_minutes)')
            
        logger.info(f"Database initialized successfully at {DB_FILE}")
        logger.info("✅ WAL mode enabled for better performance")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        stats['errors'] += 1

init_db()

def backup_database():
    """Create automatic backup of database with data integrity checks"""
    try:
        if not os.path.exists(DB_FILE):
            logger.warning("Database file not found, cannot create backup")
            return None
        
        # Ensure database is checkpointed before backup
        try:
            with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                conn.execute('PRAGMA wal_checkpoint(FULL)')
                conn.commit()
        except Exception as e:
            logger.warning(f"Pre-backup checkpoint failed: {e}")
        
        timestamp = datetime.now(IST).strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(BACKUP_DIR, f"orderflow_backup_{timestamp}.db")
        
        # Copy database file with metadata preservation
        import shutil
        shutil.copy2(DB_FILE, backup_file)
        
        # Verify backup file exists and has content
        if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
            logger.info(f"✅ Database backup created: {backup_file} ({os.path.getsize(backup_file) / 1024:.1f} KB)")
        else:
            logger.error(f"❌ Backup file is empty or missing: {backup_file}")
            return None
        
        # Keep only last 3 days of backups (reduced from 7 to save disk space)
        # Can be overridden via environment variable BACKUP_RETENTION_DAYS
        retention_days = int(os.getenv('BACKUP_RETENTION_DAYS', 3))
        try:
            for file in os.listdir(BACKUP_DIR):
                file_path = os.path.join(BACKUP_DIR, file)
                if os.path.isfile(file_path) and file.endswith('.db'):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if (datetime.now() - file_time).days > retention_days:
                        os.remove(file_path)
                        logger.info(f"Removed old backup: {file} (age: {(datetime.now() - file_time).days} days, retention: {retention_days} days)")
        except Exception as e:
            logger.warning(f"Backup cleanup failed: {e}")
        
        return backup_file
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return None

# Schedule automatic backups every hour
def backup_scheduler():
    """Run backups periodically with data safety checks"""
    # Create initial backup on startup (after 2 minutes to ensure data exists)
    time.sleep(120)  # Wait 2 minutes for app to stabilize and collect some data
    logger.info("🔄 Starting backup scheduler...")
    initial_backup = backup_database()
    if initial_backup:
        logger.info(f"✅ Initial backup created: {initial_backup}")
    else:
        logger.warning("⚠️ Initial backup failed or skipped (database may be empty)")
    
    while True:
        time.sleep(3600)  # Every hour
        logger.info("🔄 Running scheduled backup...")
        backup_result = backup_database()
        if backup_result:
            logger.info(f"✅ Scheduled backup created: {backup_result}")
        # Also flush database to ensure data persistence
        flush_db_batch()

backup_thread = threading.Thread(target=backup_scheduler, daemon=True)
backup_thread.start()

# Periodic database checkpoint (every 5 minutes) to ensure data safety
def checkpoint_scheduler():
    """Periodically checkpoint WAL to ensure data persistence"""
    while True:
        time.sleep(300)  # Every 5 minutes
        try:
            with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                logger.debug("Database checkpoint completed")
        except Exception as e:
            logger.warning(f"Checkpoint failed: {e}")

checkpoint_thread = threading.Thread(target=checkpoint_scheduler, daemon=True)
checkpoint_thread.start()

def store_in_db_batch(batch_data):
    """Store multiple records in database at once with data safety"""
    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            # Ensure WAL mode for safety
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')  # Safe but fast
            
            conn.executemany(
                "INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume, buy_initiated, sell_initiated, tick_delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_data
            )
            # Explicit commit to ensure data is written
            conn.commit()
            # Force checkpoint to ensure WAL is flushed to main database
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        
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
    """Force write remaining batch data and ensure persistence"""
    global db_batch
    
    with db_batch_lock:
        if db_batch:
            store_in_db_batch(db_batch.copy())
            db_batch.clear()
            
            # Force database sync to ensure data is on disk
            try:
                with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                    conn.execute('PRAGMA wal_checkpoint(FULL)')  # Full checkpoint for safety
                    conn.commit()
            except Exception as e:
                logger.warning(f"Checkpoint failed: {e}")

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
        # Optional: limit to last N hours (default: today only, max 24 hours)
        hours_back = int(request.args.get('hours', 24))
    except (ValueError, TypeError):
        interval = 5
        hours_back = 24
    
    try:
        # Calculate time filter (only fetch recent data)
        now = datetime.now(IST)
        start_time = now - timedelta(hours=hours_back)
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Query from SQLite DB with date filter and only needed columns
        with sqlite3.connect(DB_FILE) as conn:
            # Only select columns we need, filter by date
            query = """
                SELECT timestamp, buy_volume, sell_volume, ltp, 
                       buy_initiated, sell_initiated, tick_delta
                FROM orderflow 
                WHERE security_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
            """
            df = pd.read_sql_query(query, conn, params=(security_id, start_time_str))
        
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

        # Filter to today's data (9 AM to 11:59 PM)
        today = now.date()
        start_of_day = datetime.combine(today, dt_time(9, 0))
        end_of_day = datetime.combine(today, dt_time(23, 59, 59))
        df = df[(df['timestamp'] >= start_of_day) & (df['timestamp'] <= end_of_day)]
        
        if df.empty:
            return jsonify([])
        
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

def get_cache_key(security_id, days, start_date, end_date):
    """Generate cache key for query"""
    key_str = f"{security_id}_{days}_{start_date}_{end_date}"
    return hashlib.md5(key_str.encode()).hexdigest()

@app.route('/api/historical_data/<string:security_id>')
def get_historical_data(security_id):
    """
    Fetch all historical data for a security from SQLite.
    Optimized with caching for GoCharting-like speed.
    
    Query params:
    - days: Number of days to fetch (default: 7, max: 30)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    """
    try:
        # Get query parameters
        days = int(request.args.get('days', 7))
        days = min(days, 30)  # Limit to 30 days max for performance
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Check cache first (like GoCharting)
        cache_key = get_cache_key(security_id, days, start_date, end_date)
        with cache_lock:
            if cache_key in query_cache:
                cached_data, cached_time = query_cache[cache_key]
                if (time.time() - cached_time) < CACHE_TTL:
                    stats['api_calls'] += 1
                    return jsonify(cached_data)
        
        now = datetime.now(IST)
        
        # Build date filter
        if start_date and end_date:
            # Use provided date range
            start_time_str = f"{start_date} 00:00:00"
            end_time_str = f"{end_date} 23:59:59"
        else:
            # Use days parameter
            start_time = now - timedelta(days=days)
            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Optimized query with connection reuse
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            # Use prepared statement for better performance
            query = """
                SELECT timestamp, buy_volume, sell_volume, ltp, volume,
                       buy_initiated, sell_initiated, tick_delta
                FROM orderflow 
                WHERE security_id = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC
            """
            df = pd.read_sql_query(query, conn, params=(security_id, start_time_str, end_time_str))
        
        if df.empty:
            result = []
        else:
            # Convert timestamp to datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                df = df.dropna(subset=['timestamp'])
            
            # Convert to list of dicts for JSON response
            result = df.to_dict('records')
            
            # Convert datetime to string for JSON serialization
            for record in result:
                if pd.notna(record['timestamp']):
                    record['timestamp'] = record['timestamp'].isoformat()
        
        # Cache the result (like GoCharting)
        with cache_lock:
            query_cache[cache_key] = (result, time.time())
            # Limit cache size (keep last 100 queries)
            if len(query_cache) > 100:
                oldest_key = min(query_cache.items(), key=lambda x: x[1][1])[0]
                del query_cache[oldest_key]
        
        stats['api_calls'] += 1
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in get_historical_data for {security_id}: {e}")
        stats['errors'] += 1
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/cumulative_delta/<string:security_id>')
def get_cumulative_tick_delta(security_id):
    try:
        interval = int(request.args.get('interval', 5))

        # Filter to today's data only for better performance
        today = datetime.now(IST).date()
        start_time_str = today.strftime('%Y-%m-%d 09:00:00')
        end_time_str = today.strftime('%Y-%m-%d 23:59:59')

        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql_query(
                "SELECT timestamp, tick_delta FROM orderflow WHERE security_id = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC",
                conn, params=(security_id, start_time_str, end_time_str)
            )

        if df.empty or 'tick_delta' not in df.columns:
            return jsonify({"security_id": security_id, "cumulative_tick_delta": 0})

        # Convert timestamp if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

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
    # Check database health
    db_healthy = False
    db_size = 0
    try:
        if os.path.exists(DB_FILE):
            db_size = os.path.getsize(DB_FILE) / (1024 * 1024)  # Size in MB
            with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                conn.execute("SELECT 1")
                db_healthy = True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
    
    # Count backup files safely
    backups_count = 0
    try:
        if os.path.exists(BACKUP_DIR):
            backups_count = len([f for f in os.listdir(BACKUP_DIR) if f.endswith('.db') and 'backup' in f.lower()])
    except Exception as e:
        logger.warning(f"Failed to count backups: {e}")
    
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(IST).isoformat(),
        "securities_count": len(live_market_data),
        "database": {
            "healthy": db_healthy,
            "size_mb": round(db_size, 2),
            "location": DB_FILE,
            "backup_dir": BACKUP_DIR
        },
        "cache_size": len(query_cache),
        "backups_count": backups_count,
        "backup_status": "active" if backups_count > 0 or (datetime.now(IST) - datetime.fromisoformat(stats['start_time'])).total_seconds() < 180 else "pending"
    })

@app.route('/api/cache/clear')
def clear_cache():
    """Clear query cache (admin endpoint)"""
    global query_cache
    with cache_lock:
        cache_size = len(query_cache)
        query_cache.clear()
    return jsonify({
        "status": "success",
        "cleared_entries": cache_size,
        "message": "Cache cleared successfully"
    })

@app.route('/api/backup/trigger')
def trigger_backup():
    """Manually trigger database backup"""
    backup_file = backup_database()
    if backup_file:
        return jsonify({
            "status": "success",
            "backup_file": backup_file,
            "message": "Backup created successfully"
        })
    else:
        return jsonify({
            "status": "error",
            "message": "Backup failed"
        }), 500

@app.route('/api/disk/usage')
def get_disk_usage():
    """Get disk usage statistics"""
    try:
        import shutil
        
        # Get disk usage for persistent directory
        if os.path.exists(PERSISTENT_DIR):
            total, used, free = shutil.disk_usage(PERSISTENT_DIR)
        else:
            # Fallback to current directory
            total, used, free = shutil.disk_usage(os.path.dirname(os.path.abspath(__file__)))
        
        # Database size
        db_size = 0
        if os.path.exists(DB_FILE):
            db_size = os.path.getsize(DB_FILE)
        
        # WAL file size (if exists)
        wal_size = 0
        wal_file = DB_FILE + '-wal'
        if os.path.exists(wal_file):
            wal_size = os.path.getsize(wal_file)
        
        # Backup directory size
        backup_size = 0
        backup_count = 0
        if os.path.exists(BACKUP_DIR):
            for file in os.listdir(BACKUP_DIR):
                file_path = os.path.join(BACKUP_DIR, file)
                if os.path.isfile(file_path):
                    backup_size += os.path.getsize(file_path)
                    backup_count += 1
        
        # Count database records
        record_count = 0
        try:
            with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM orderflow")
                record_count = cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"Failed to count records: {e}")
        
        return jsonify({
            "status": "success",
            "disk": {
                "total_gb": round(total / (1024**3), 2),
                "used_gb": round(used / (1024**3), 2),
                "free_gb": round(free / (1024**3), 2),
                "used_percent": round((used / total) * 100, 2)
            },
            "database": {
                "size_mb": round(db_size / (1024**2), 2),
                "wal_size_mb": round(wal_size / (1024**2), 2),
                "record_count": record_count,
                "location": DB_FILE
            },
            "backups": {
                "count": backup_count,
                "total_size_mb": round(backup_size / (1024**2), 2),
                "location": BACKUP_DIR
            },
            "persistent_dir": PERSISTENT_DIR
        })
    except Exception as e:
        logger.error(f"Failed to get disk usage: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/disk/cleanup/backups')
def cleanup_backups():
    """Clean old backup files (keep only last N days)"""
    try:
        days_to_keep = int(request.args.get('days', 3))  # Default: keep only 3 days
        days_to_keep = max(1, min(days_to_keep, 30))  # Between 1 and 30 days
        
        if not os.path.exists(BACKUP_DIR):
            return jsonify({
                "status": "success",
                "message": "No backup directory found",
                "deleted_count": 0
            })
        
        deleted_count = 0
        deleted_size = 0
        kept_count = 0
        
        for file in os.listdir(BACKUP_DIR):
            file_path = os.path.join(BACKUP_DIR, file)
            if os.path.isfile(file_path) and file.endswith('.db') and 'backup' in file.lower():
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                age_days = (datetime.now() - file_time).days
                
                if age_days > days_to_keep:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    deleted_count += 1
                    deleted_size += file_size
                    logger.info(f"Deleted old backup: {file} (age: {age_days} days)")
                else:
                    kept_count += 1
        
        return jsonify({
            "status": "success",
            "deleted_count": deleted_count,
            "deleted_size_mb": round(deleted_size / (1024**2), 2),
            "kept_count": kept_count,
            "days_kept": days_to_keep,
            "message": f"Cleaned up {deleted_count} old backup(s), kept {kept_count} backup(s)"
        })
    except Exception as e:
        logger.error(f"Backup cleanup failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/disk/cleanup/database')
def cleanup_database():
    """Clean old database records (keep only last N days)"""
    try:
        days_to_keep = int(request.args.get('days', 7))  # Default: keep 7 days
        days_to_keep = max(1, min(days_to_keep, 90))  # Between 1 and 90 days
        
        cutoff_date = datetime.now(IST) - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get count before deletion
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM orderflow WHERE timestamp < ?", (cutoff_str,))
            records_to_delete = cursor.fetchone()[0]
            
            # Delete old records
            conn.execute("DELETE FROM orderflow WHERE timestamp < ?", (cutoff_str,))
            conn.commit()
            
            # Vacuum to reclaim space
            conn.execute("VACUUM")
            conn.commit()
            
            # Get new count
            cursor = conn.execute("SELECT COUNT(*) FROM orderflow")
            remaining_records = cursor.fetchone()[0]
        
        logger.info(f"Cleaned up {records_to_delete} old database records, kept {remaining_records} records")
        
        return jsonify({
            "status": "success",
            "deleted_records": records_to_delete,
            "remaining_records": remaining_records,
            "days_kept": days_to_keep,
            "cutoff_date": cutoff_str,
            "message": f"Deleted {records_to_delete} old records, kept last {days_to_keep} days"
        })
    except Exception as e:
        logger.error(f"Database cleanup failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/disk/vacuum')
def vacuum_database():
    """Vacuum database to reclaim disk space"""
    try:
        # Get size before vacuum
        size_before = 0
        if os.path.exists(DB_FILE):
            size_before = os.path.getsize(DB_FILE)
        
        # Vacuum database
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            # Checkpoint WAL first
            conn.execute('PRAGMA wal_checkpoint(FULL)')
            conn.commit()
            
            # Vacuum to reclaim space
            conn.execute('VACUUM')
            conn.commit()
        
        # Get size after vacuum
        size_after = 0
        if os.path.exists(DB_FILE):
            size_after = os.path.getsize(DB_FILE)
        
        space_reclaimed = size_before - size_after
        
        logger.info(f"Database vacuumed: reclaimed {space_reclaimed / (1024**2):.2f} MB")
        
        return jsonify({
            "status": "success",
            "size_before_mb": round(size_before / (1024**2), 2),
            "size_after_mb": round(size_after / (1024**2), 2),
            "space_reclaimed_mb": round(space_reclaimed / (1024**2), 2),
            "message": f"Database vacuumed successfully, reclaimed {round(space_reclaimed / (1024**2), 2)} MB"
        })
    except Exception as e:
        logger.error(f"Database vacuum failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/disk/cleanup')
def full_cleanup():
    """Perform full disk cleanup (backups + database + vacuum)"""
    try:
        backup_days = int(request.args.get('backup_days', 3))  # Keep 3 days of backups
        db_days = int(request.args.get('db_days', 7))  # Keep 7 days of database records
        
        results = {
            "backups": {},
            "database": {},
            "vacuum": {}
        }
        
        # Clean backups
        try:
            backup_response = cleanup_backups()
            if backup_response.status_code == 200:
                results["backups"] = backup_response.get_json()
        except Exception as e:
            results["backups"] = {"status": "error", "message": str(e)}
        
        # Clean database
        try:
            db_response = cleanup_database()
            if db_response.status_code == 200:
                results["database"] = db_response.get_json()
        except Exception as e:
            results["database"] = {"status": "error", "message": str(e)}
        
        # Vacuum database
        try:
            vacuum_response = vacuum_database()
            if vacuum_response.status_code == 200:
                results["vacuum"] = vacuum_response.get_json()
        except Exception as e:
            results["vacuum"] = {"status": "error", "message": str(e)}
        
        # Get final disk usage
        usage_response = get_disk_usage()
        final_usage = usage_response.get_json() if usage_response.status_code == 200 else {}
        
        return jsonify({
            "status": "success",
            "cleanup_results": results,
            "final_disk_usage": final_usage,
            "message": "Full cleanup completed"
        })
    except Exception as e:
        logger.error(f"Full cleanup failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

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

# Graceful shutdown with data safety
import atexit
import signal

def cleanup():
    """Ensure all data is safely written before shutdown"""
    logger.info("Shutting down gracefully...")
    
    # Flush all pending writes
    flush_db_batch()
    
    # Final checkpoint
    try:
        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            conn.execute('PRAGMA wal_checkpoint(FULL)')
            conn.commit()
        logger.info("✅ Final database checkpoint completed")
    except Exception as e:
        logger.error(f"Final checkpoint failed: {e}")
    
    # Create emergency backup
    try:
        backup_file = backup_database()
        if backup_file:
            logger.info(f"✅ Emergency backup created: {backup_file}")
    except Exception as e:
        logger.error(f"Emergency backup failed: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    cleanup()
    exit(0)

# Register cleanup handlers
atexit.register(cleanup)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=5000)
