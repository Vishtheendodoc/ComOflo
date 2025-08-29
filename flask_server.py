from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
import time
import os
import logging
from datetime import datetime, timedelta
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import sqlite3
import pandas as pd
import random

# --- CONFIG ---
API_BATCH_SIZE = 3          # Reduced for rate limiting
BATCH_INTERVAL_SEC = 10     # Increased interval
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# --- RATE LIMITING CONFIG ---
RATE_LIMIT_CONFIG = {
    'max_reconnect_attempts': 8,        # Reduced for Render
    'base_backoff_seconds': 30,         # Increased base delay
    'max_backoff_seconds': 600,         # 10 minutes max
    'exponential_backoff': True,
    'jitter': True,
    'rate_limit_window': 60,            # 1 minute window
    'max_requests_per_window': 50,      # Conservative for Dhan API
    'connection_retry_delay': 60,       # 1 minute between retries
    'max_instruments_per_connection': 2000,  # Reduced for stability
    'data_fetch_delay': 0.1,            # Slower data fetching
    'health_check_interval': 300        # 5 minutes
}

# --- LOGGING SETUP ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
ENVIRONMENT = os.getenv('ENVIRONMENT', 'production')  # Default to production for Render

# Configure logging with reduced verbosity for production
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce logging noise in production
if ENVIRONMENT == 'production':
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)

# Set Flask debug mode based on environment
app.config['DEBUG'] = ENVIRONMENT == 'development'

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
    'start_time': datetime.now().isoformat(),
    'connection_attempts': 0,
    'last_successful_connection': None,
    'rate_limit_hits': 0,
    'current_instruments': 0
}

# Batch processing for database writes
db_batch = []
DB_BATCH_SIZE = 50  # Reduced for Render
db_batch_lock = threading.Lock()

# Connection health tracking
connection_health = {
    'is_connected': False,
    'last_data_received': None,
    'consecutive_errors': 0,
    'current_retry_count': 0
}

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU4OTQ5MjcwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.xjTrEBxvCxX8kf6P3ZrfvJupkdNBNhL9A2Tf9jrz4UkQ52rz2Jzqj9kX1POdYULPuJp6dFYQ68TL3kQWZImvAg"

try:
    analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)
    logger.info("OrderFlowAnalyzer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize OrderFlowAnalyzer: {e}")
    analyzer = None

def get_instrument_list():
    """Load and limit instrument list for rate limiting compliance"""
    stocks = []
    try:
        with open(STOCK_LIST_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            instruments = list(reader)
            
            # Limit instruments based on environment and rate limits
            max_instruments = RATE_LIMIT_CONFIG['max_instruments_per_connection']
            if ENVIRONMENT == 'production':
                max_instruments = min(max_instruments, 1000)  # Even more conservative for Render
            
            limited_instruments = instruments[:max_instruments]
            
            for row in limited_instruments:
                try:
                    exch = row.get("exchange", "").strip()
                    seg = row.get("segment", "").strip()
                    sec_id = str(row.get("security_id", "")).strip()
                    
                    if not sec_id:
                        continue
                        
                    # Map to MarketFeed enums/constants
                    if exch == "NSE" and seg == "D":
                        stocks.append((MarketFeed.NSE_FNO, sec_id, MarketFeed.Quote))
                    elif exch == "MCX" and seg == "M":
                        stocks.append(("MCX_COMM", sec_id, MarketFeed.Quote))
                except Exception as e:
                    logger.warning(f"Skipping invalid instrument row: {e}")
                    continue
                    
        stats['current_instruments'] = len(stocks)
        logger.info(f"Loaded {len(stocks)} instruments from {STOCK_LIST_FILE} (limited from {len(instruments)} total)")
        
    except FileNotFoundError:
        logger.error(f"Stock list file {STOCK_LIST_FILE} not found")
        stats['errors'] += 1
    except Exception as e:
        logger.error(f"Failed to load instrument list: {e}")
        stats['errors'] += 1
    return stocks

# Initialize instruments and market feed
instrument_list = get_instrument_list()

dhan_context = None
market_feed = None

try:
    if instrument_list:
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        market_feed = MarketFeed(dhan_context, instrument_list, "v2")
        logger.info("Market feed initialized successfully")
    else:
        logger.error("No instruments loaded, market feed not initialized")
except Exception as e:
    logger.error(f"Failed to initialize market feed: {e}")
    stats['errors'] += 1

def init_db():
    """Initialize SQLite database with proper error handling"""
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
            conn.execute('PRAGMA journal_mode=WAL')  # Better for concurrent access
            conn.execute('PRAGMA synchronous=NORMAL')  # Balance between speed and safety
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        stats['errors'] += 1

init_db()

def store_in_db_batch(batch_data):
    """Store multiple records in database at once with error handling"""
    if not batch_data:
        return
        
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
    
    try:
        with db_batch_lock:
            db_batch.append((security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta))
            
            # Write batch when it reaches the threshold
            if len(db_batch) >= DB_BATCH_SIZE:
                store_in_db_batch(db_batch.copy())
                db_batch.clear()
    except Exception as e:
        logger.error(f"Failed to add to DB batch: {e}")
        stats['errors'] += 1

def flush_db_batch():
    """Force write remaining batch data"""
    global db_batch
    
    try:
        with db_batch_lock:
            if db_batch:
                store_in_db_batch(db_batch.copy())
                db_batch.clear()
    except Exception as e:
        logger.error(f"Failed to flush DB batch: {e}")
        stats['errors'] += 1

class RateLimitedMarketFeed:
    """Rate limited market feed handler with exponential backoff"""
    
    def __init__(self, market_feed, logger):
        self.market_feed = market_feed
        self.logger = logger
        self.reconnect_count = 0
        self.last_successful_connection = None
        self.request_timestamps = []
        self.is_rate_limited = False
        self.rate_limit_until = None
        
    def _cleanup_old_requests(self):
        """Remove request timestamps older than the rate limit window"""
        cutoff = datetime.now() - timedelta(seconds=RATE_LIMIT_CONFIG['rate_limit_window'])
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > cutoff]
    
    def _can_make_request(self):
        """Check if we can make a request without hitting rate limits"""
        self._cleanup_old_requests()
        
        # Check if we're still in a rate limit period
        if self.rate_limit_until and datetime.now() < self.rate_limit_until:
            return False
            
        return len(self.request_timestamps) < RATE_LIMIT_CONFIG['max_requests_per_window']
    
    def _record_request(self):
        """Record a request timestamp"""
        self.request_timestamps.append(datetime.now())
    
    def _calculate_backoff_delay(self):
        """Calculate exponential backoff delay with jitter"""
        if RATE_LIMIT_CONFIG['exponential_backoff']:
            delay = min(
                RATE_LIMIT_CONFIG['base_backoff_seconds'] * (2 ** min(self.reconnect_count, 5)),
                RATE_LIMIT_CONFIG['max_backoff_seconds']
            )
        else:
            delay = RATE_LIMIT_CONFIG['base_backoff_seconds']
        
        # Add jitter to prevent thundering herd
        if RATE_LIMIT_CONFIG['jitter']:
            delay += random.uniform(0, delay * 0.1)
        
        return delay
    
    def _handle_429_error(self):
        """Handle rate limit error"""
        self.is_rate_limited = True
        # Set rate limit period - be very conservative
        self.rate_limit_until = datetime.now() + timedelta(minutes=10)
        self.reconnect_count += 1
        stats['rate_limit_hits'] += 1
        
        delay = self._calculate_backoff_delay()
        self.logger.warning(f"Rate limited (429). Backing off for {delay:.1f} seconds. Attempt {self.reconnect_count}")
        
        return delay
    
    def connect_with_rate_limiting(self):
        """Connect to market feed with rate limiting and exponential backoff"""
        connection_health['current_retry_count'] = self.reconnect_count
        
        while self.reconnect_count < RATE_LIMIT_CONFIG['max_reconnect_attempts']:
            try:
                # Check if we can make a request
                if not self._can_make_request():
                    wait_time = RATE_LIMIT_CONFIG['rate_limit_window']
                    self.logger.info(f"Rate limit preventive wait: {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                
                self.logger.info(f"Attempting to connect to market feed (attempt {self.reconnect_count + 1})")
                self._record_request()
                stats['connection_attempts'] += 1
                
                # Try to connect
                self.market_feed.run_forever()
                
                # If we get here, connection was successful
                self.logger.info("Successfully connected to market feed")
                self.reconnect_count = 0  # Reset on successful connection
                self.is_rate_limited = False
                self.rate_limit_until = None
                self.last_successful_connection = datetime.now()
                stats['last_successful_connection'] = self.last_successful_connection.isoformat()
                connection_health['is_connected'] = True
                connection_health['consecutive_errors'] = 0
                
                return True
                
            except Exception as e:
                error_msg = str(e).lower()
                connection_health['is_connected'] = False
                connection_health['consecutive_errors'] += 1
                
                if "429" in error_msg or "rate limit" in error_msg or "too many requests" in error_msg:
                    delay = self._handle_429_error()
                    time.sleep(delay)
                    
                elif "connection" in error_msg or "network" in error_msg or "timeout" in error_msg:
                    # Network/connection error - longer backoff for stability
                    self.reconnect_count += 1
                    delay = min(RATE_LIMIT_CONFIG['connection_retry_delay'] * 2, self._calculate_backoff_delay())
                    self.logger.error(f"Connection error: {e}. Retrying in {delay:.1f} seconds")
                    time.sleep(delay)
                    
                else:
                    # Other errors
                    self.reconnect_count += 1
                    delay = self._calculate_backoff_delay()
                    self.logger.error(f"Market feed error: {e}. Retrying in {delay:.1f} seconds")
                    time.sleep(delay)
        
        self.logger.error(f"Failed to connect after {RATE_LIMIT_CONFIG['max_reconnect_attempts']} attempts")
        connection_health['is_connected'] = False
        return False

def process_market_data(response):
    """Process market data response with error handling"""
    global stats, live_market_data, orderflow_history, prev_ltp, prev_volume
    
    try:
        if not response or not isinstance(response, dict):
            return
            
        required_keys = ["security_id"]
        missing_keys = [key for key in required_keys if key not in response]
        
        if missing_keys:
            logger.debug(f"Missing keys {missing_keys} in response")
            return
        
        security_id = str(response.get("security_id"))
        if not security_id:
            logger.debug("Empty security_id in response")
            return

        # Update stats
        stats['messages_processed'] += 1
        connection_health['last_data_received'] = datetime.now()
        
        # Periodic logging instead of per-message logging
        if stats['messages_processed'] % 2000 == 0:  # Increased interval
            logger.info(f"Processed {stats['messages_processed']} messages, {stats['errors']} errors, {stats['db_writes']} DB writes")

        # Update live data
        live_market_data[security_id] = response
        
        # Update history with size management
        if security_id not in orderflow_history:
            orderflow_history[security_id] = []
        orderflow_history[security_id].append(response)
        
        # Keep history size very manageable for Render
        if len(orderflow_history[security_id]) > 5000:
            orderflow_history[security_id] = orderflow_history[security_id][-2500:]
        
        # Store Quote Data in DB (batched)
        if response.get('type') == 'Quote Data':
            process_quote_data(response, security_id)
            
    except Exception as e:
        logger.error(f"Error processing market data: {e}")
        stats['errors'] += 1

def process_quote_data(response, security_id):
    """Process quote data and store in database"""
    global prev_ltp, prev_volume
    
    try:
        ltt = response.get("LTT")
        today = datetime.now().strftime('%Y-%m-%d')
        timestamp = f"{today} {ltt}" if ltt else datetime.now().isoformat()
        buy = response.get("total_buy_quantity", 0)
        sell = response.get("total_sell_quantity", 0)
        ltp_val = float(response.get("LTP", 0)) if response.get("LTP") else 0
        volume = response.get("volume", 0)
        
        # Tick-rule logic with error handling
        prev = prev_ltp.get(security_id)
        prev_total_volume = prev_volume.get(security_id, 0)
        current_total_volume = volume
        delta_volume = max(0, current_total_volume - prev_total_volume)  # Ensure non-negative
        prev_volume[security_id] = current_total_volume

        buy_initiated = 0
        sell_initiated = 0

        if prev is not None and delta_volume > 0 and ltp_val > 0:
            if ltp_val > prev:
                buy_initiated = delta_volume
            elif ltp_val < prev:
                sell_initiated = delta_volume
            else:
                # Price flat → use Bid/Ask proximity
                bid_price = response.get("bid_price", 0)
                ask_price = response.get("ask_price", 0)

                if bid_price and ask_price and bid_price < ask_price:
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

        tick_delta = buy_initiated - sell_initiated
        if ltp_val > 0:  # Only update if we have a valid LTP
            prev_ltp[security_id] = ltp_val
        
        # Add to batch for database storage
        add_to_db_batch(security_id, timestamp, buy, sell, ltp_val, volume, buy_initiated, sell_initiated, tick_delta)
        
    except Exception as e:
        logger.error(f"Error processing quote data for {security_id}: {e}")
        stats['errors'] += 1

def improved_marketfeed_thread():
    """Improved market feed thread with proper rate limiting for Render"""
    global stats, connection_health
    
    if not market_feed:
        logger.error("Market feed not initialized. Cannot start market feed thread.")
        return
    
    rate_limited_feed = RateLimitedMarketFeed(market_feed, logger)
    
    while True:
        try:
            # Connect with rate limiting
            if not rate_limited_feed.connect_with_rate_limiting():
                logger.critical("Unable to establish market feed connection. Waiting before retry...")
                time.sleep(300)  # Wait 5 minutes before trying again
                continue
            
            # Process data with rate limiting awareness
            consecutive_errors = 0
            last_data_time = datetime.now()
            no_data_warning_sent = False
            
            while connection_health['is_connected']:
                try:
                    # Check if we can make a request
                    if not rate_limited_feed._can_make_request():
                        time.sleep(2)  # Wait if we're hitting rate limits
                        continue
                    
                    response = market_feed.get_data()
                    if response:
                        rate_limited_feed._record_request()
                    
                    if response and isinstance(response, dict):
                        # Reset error counter on successful data
                        consecutive_errors = 0
                        last_data_time = datetime.now()
                        no_data_warning_sent = False
                        connection_health['consecutive_errors'] = 0
                        
                        # Process the response
                        process_market_data(response)
                        
                    else:
                        # Check if we've been without data for too long
                        time_without_data = (datetime.now() - last_data_time).total_seconds()
                        
                        if time_without_data > 600 and not no_data_warning_sent:  # 10 minutes
                            logger.warning(f"No market data received for {time_without_data:.0f} seconds")
                            no_data_warning_sent = True
                        
                        if time_without_data > 1800:  # 30 minutes
                            logger.warning("No market data for 30 minutes. Reconnecting...")
                            connection_health['is_connected'] = False
                            break
                    
                    # Adaptive sleep based on data flow and environment
                    sleep_time = RATE_LIMIT_CONFIG['data_fetch_delay']
                    if not response:
                        sleep_time *= 2  # Longer wait if no data
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    consecutive_errors += 1
                    connection_health['consecutive_errors'] = consecutive_errors
                    error_msg = str(e).lower()
                    
                    if "429" in error_msg or "rate limit" in error_msg:
                        logger.warning("Rate limit hit during data processing")
                        rate_limited_feed._handle_429_error()
                        connection_health['is_connected'] = False
                        break  # Break to reconnect with backoff
                    
                    elif consecutive_errors > 20:  # Increased threshold
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Reconnecting...")
                        connection_health['is_connected'] = False
                        break
                    
                    else:
                        logger.error(f"Data processing error: {e}")
                        stats['errors'] += 1
                        time.sleep(5)  # Wait before retrying
        
        except Exception as e:
            logger.error(f"Market feed thread crashed: {e}")
            stats['errors'] += 1
            connection_health['is_connected'] = False
            time.sleep(60)  # Wait 1 minute before restarting entire thread

def monitor_connection_health():
    """Monitor connection health and provide metrics"""
    while True:
        try:
            # Log connection statistics every 10 minutes for production
            time.sleep(RATE_LIMIT_CONFIG['health_check_interval'])
            
            current_time = datetime.now()
            logger.info("=== CONNECTION HEALTH CHECK ===")
            logger.info(f"Time: {current_time}")
            logger.info(f"Connected: {connection_health['is_connected']}")
            logger.info(f"Messages processed: {stats['messages_processed']}")
            logger.info(f"Errors: {stats['errors']}")
            logger.info(f"Rate limit hits: {stats['rate_limit_hits']}")
            logger.info(f"Live securities: {len(live_market_data)}")
            logger.info(f"DB writes: {stats['db_writes']}")
            logger.info(f"Instruments loaded: {stats['current_instruments']}")
            
            if connection_health['last_data_received']:
                time_since_data = (current_time - connection_health['last_data_received']).total_seconds()
                logger.info(f"Seconds since last data: {time_since_data:.0f}")
            
            # Memory cleanup
            if len(live_market_data) > 10000:  # Clean up if too much data
                logger.info("Performing memory cleanup...")
                # Keep only recent data for half the securities
                securities = list(live_market_data.keys())
                for sec_id in securities[::2]:  # Every second security
                    if sec_id in orderflow_history and len(orderflow_history[sec_id]) > 1000:
                        orderflow_history[sec_id] = orderflow_history[sec_id][-500:]
            
        except Exception as e:
            logger.error(f"Health monitor error: {e}")
            time.sleep(120)  # Wait 2 minutes on error

def start_improved_marketfeed():
    """Start the improved market feed with health monitoring"""
    if not market_feed:
        logger.error("Cannot start market feed - not initialized")
        return
        
    # Start the main market feed thread
    marketfeed_thread_obj = threading.Thread(target=improved_marketfeed_thread, daemon=True)
    marketfeed_thread_obj.start()
    
    # Start connection health monitor
    health_monitor_thread = threading.Thread(target=monitor_connection_health, daemon=True)
    health_monitor_thread.start()
    
    logger.info("Started improved market feed with rate limiting")

# Periodic batch flush thread
def batch_flush_thread():
    """Flush database batches periodically"""
    while True:
        try:
            time.sleep(60)  # Flush every minute
            flush_db_batch()
        except Exception as e:
            logger.error(f"Batch flush error: {e}")
            time.sleep(30)

# Start background threads
threading.Thread(target=batch_flush_thread, daemon=True).start()

# Start the improved market feed
start_improved_marketfeed()

def maybe_reset_history():
    """Reset history daily"""
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')

    if last_reset_date[0] != today_str and now.strftime('%H:%M') >= RESET_TIME:
        delta_history.clear()
        last_reset_date[0] = today_str
        logger.info(f"Cleared delta history at {now.strftime('%H:%M:%S')}")

def load_stock_list():
    """Load stock list for API responses"""
    stocks = []
    try:
        with open(STOCK_LIST_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get("security_id") and row.get("symbol"):
                    stocks.append((row["security_id"], row["symbol"]))
        logger.info(f"Loaded {len(stocks)} stocks from {STOCK_LIST_FILE}")
    except Exception as e:
        logger.error(f"Failed to load stock list: {e}")
        stats['errors'] += 1
    return stocks

# --- FLASK ROUTES ---

@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    """Get delta data for a security with improved error handling"""
    try:
        interval = max(1, min(60, int(request.args.get('interval', 5))))  # Limit interval range
    except (ValueError, TypeError):
        interval = 5
    
    try:
        # Query from SQLite DB with timeout
        with sqlite3.connect(DB_FILE, timeout=30) as conn:
            # Limit query to last 24 hours for performance
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            df = pd.read_sql_query(
                "SELECT * FROM orderflow WHERE security_id = ? AND timestamp >= ? ORDER BY timestamp ASC LIMIT 10000",
                conn, params=(security_id, yesterday)
            )
        
        if df.empty:
            return jsonify([])
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                df = df.dropna(subset=['timestamp'])
            except Exception:
                logger.error(f"Timestamp parsing failed for security {security_id}")
                return jsonify([])

        buckets = []
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        
        for bucket_label, group in grouped:
            if group.empty:
                continue
                
            if len(group) >= 2:
                start = group.iloc[0]
                end = group.iloc[-1]
                buy_delta = max(0, end['buy_volume'] - start['buy_volume'])
                sell_delta = max(0, end['sell_volume'] - start['sell_volume'])
                delta = buy_delta - sell_delta
                
                # Tick-rule aggregation
                buy_initiated_sum = group['buy_initiated'].fillna(0).sum()
                sell_initiated_sum = group['sell_initiated'].fillna(0).sum()
                tick_delta_sum = group['tick_delta'].fillna(0).sum()
                
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
                    open_ = float(ohlc.iloc[0])
                    high_ = float(ohlc.max())
                    low_ = float(ohlc.min())
                    close_ = float(ohlc.iloc[-1])
                else:
                    open_ = high_ = low_ = close_ = None
                
                buckets.append({
                    'timestamp': bucket_label.strftime('%H:%M'),
                    'buy_volume': float(buy_delta),
                    'sell_volume': float(sell_delta),
                    'delta': float(delta),
                    'buy_initiated': float(buy_initiated_sum),
                    'sell_initiated': float(sell_initiated_sum),
                    'tick_delta': float(tick_delta_sum),
                    'inference': inference,
                    'open': open_,
                    'high': high_,
                    'low': low_,
                    'close': close_
                })
            elif len(group) == 1:
                row = group.iloc[0]
                ohlc = float(row['ltp']) if pd.notnull(row['ltp']) else None
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
    """Get cumulative tick delta for a security"""
    try:
        interval = max(1, min(60, int(request.args.get('interval', 5))))

        with sqlite3.connect(DB_FILE, timeout=30) as conn:
            # Limit to today's data
            today = datetime.now().strftime('%Y-%m-%d')
            df = pd.read_sql_query(
                "SELECT timestamp, tick_delta FROM orderflow WHERE security_id = ? AND date(timestamp) = ? ORDER BY timestamp ASC",
                conn, params=(security_id, today)
            )

        if df.empty or 'tick_delta' not in df.columns:
            return jsonify({"security_id": security_id, "cumulative_tick_delta": 0})

        # Convert timestamp if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

        # Resample by interval
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        tick_deltas = [group['tick_delta'].fillna(0).sum() for _, group in grouped if not group.empty]

        # Calculate cumulative tick delta
        cumulative_tick_delta = sum(tick_deltas)

        return jsonify({
            "security_id": security_id,
            "interval": interval,
            "cumulative_tick_delta": float(cumulative_tick_delta)
        })

    except Exception as e:
        logger.error(f"Error in get_cumulative_tick_delta for {security_id}: {e}")
        stats['errors'] += 1
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/stocks')
def get_stock_list():
    """Get list of available stocks"""
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
    """Get live market data for a security"""
    stats['api_calls'] += 1
    data = live_market_data.get(security_id)
    
    if not data:
        logger.debug(f"No live data available for security_id: {security_id}")
        return jsonify({"error": "No live data"}), 404
    
    # Clean the response to avoid serialization issues
    try:
        cleaned_data = {}
        for key, value in data.items():
            if value is not None:
                if isinstance(value, (int, float, str, bool)):
                    cleaned_data[key] = value
                elif isinstance(value, (list, dict)):
                    cleaned_data[key] = value
                else:
                    cleaned_data[key] = str(value)
        
        return jsonify(cleaned_data)
    except Exception as e:
        logger.error(f"Error serializing live data for {security_id}: {e}")
        return jsonify({"error": "Data serialization error"}), 500

@app.route('/api/orderflow_history/<string:security_id>')
def get_orderflow_history(security_id):
    """Get orderflow history for a security"""
    stats['api_calls'] += 1
    data = orderflow_history.get(security_id, [])
    
    # Limit response size for Render
    if len(data) > 1000:
        data = data[-1000:]  # Return last 1000 records only
    
    return jsonify(data)

@app.route('/api/stats')
def get_stats():
    """Monitoring endpoint for system health"""
    current_stats = stats.copy()
    
    try:
        current_stats['uptime_seconds'] = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
        current_stats['db_batch_size'] = len(db_batch)
        current_stats['live_securities'] = len(live_market_data)
        current_stats['connection_health'] = connection_health.copy()
        
        # Convert datetime objects to strings for JSON serialization
        if current_stats['connection_health']['last_data_received']:
            current_stats['connection_health']['last_data_received'] = current_stats['connection_health']['last_data_received'].isoformat()
        
        # Memory usage estimation
        current_stats['memory_estimate'] = {
            'live_data_entries': len(live_market_data),
            'history_entries': sum(len(v) for v in orderflow_history.values()),
            'total_history_securities': len(orderflow_history)
        }
        
    except Exception as e:
        logger.error(f"Error generating stats: {e}")
        current_stats['stats_error'] = str(e)
    
    return jsonify(current_stats)

@app.route('/api/health')
def health_check():
    """Health check endpoint for monitoring"""
    health_status = "healthy"
    
    # Check various health indicators
    if connection_health['consecutive_errors'] > 50:
        health_status = "degraded"
    if not connection_health['is_connected']:
        health_status = "disconnected"
    if stats['errors'] > stats['messages_processed'] * 0.1:  # More than 10% error rate
        health_status = "high_error_rate"
    
    return jsonify({
        "status": health_status,
        "timestamp": datetime.now().isoformat(),
        "securities_count": len(live_market_data),
        "is_connected": connection_health['is_connected'],
        "errors": stats['errors'],
        "messages_processed": stats['messages_processed'],
        "uptime": (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    })

@app.route('/api/reset_connection')
def reset_connection():
    """Manual endpoint to reset connection (for debugging)"""
    if ENVIRONMENT == 'development':
        try:
            connection_health['is_connected'] = False
            connection_health['consecutive_errors'] = 0
            logger.info("Connection reset requested via API")
            return jsonify({"message": "Connection reset initiated"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Not available in production"}), 403

@app.route('/')
def dashboard():
    """Main dashboard"""
    uptime = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    uptime_str = f"{uptime/3600:.1f} hours" if uptime > 3600 else f"{uptime/60:.1f} minutes"
    
    status_color = "green" if connection_health['is_connected'] else "red"
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Order Flow Flask Server</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
            .container {{ max-width: 800px; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .status {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
            .connected {{ background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
            .disconnected {{ background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 20px; }}
            .stat-item {{ background: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #007bff; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
            .links {{ margin-top: 30px; }}
            .links a {{ margin-right: 15px; padding: 8px 16px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }}
            .links a:hover {{ background: #0056b3; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 Order Flow Flask Server</h1>
            
            <div class="status {'connected' if connection_health['is_connected'] else 'disconnected'}">
                <strong>Status:</strong> {'🟢 Connected' if connection_health['is_connected'] else '🔴 Disconnected'}
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div>Environment</div>
                    <div class="stat-value">{ENVIRONMENT}</div>
                </div>
                <div class="stat-item">
                    <div>Uptime</div>
                    <div class="stat-value">{uptime_str}</div>
                </div>
                <div class="stat-item">
                    <div>Messages Processed</div>
                    <div class="stat-value">{stats['messages_processed']:,}</div>
                </div>
                <div class="stat-item">
                    <div>API Calls</div>
                    <div class="stat-value">{stats['api_calls']:,}</div>
                </div>
                <div class="stat-item">
                    <div>Errors</div>
                    <div class="stat-value">{stats['errors']:,}</div>
                </div>
                <div class="stat-item">
                    <div>DB Writes</div>
                    <div class="stat-value">{stats['db_writes']:,}</div>
                </div>
                <div class="stat-item">
                    <div>Rate Limit Hits</div>
                    <div class="stat-value">{stats['rate_limit_hits']}</div>
                </div>
                <div class="stat-item">
                    <div>Live Securities</div>
                    <div class="stat-value">{len(live_market_data)}</div>
                </div>
                <div class="stat-item">
                    <div>Instruments Loaded</div>
                    <div class="stat-value">{stats['current_instruments']}</div>
                </div>
                <div class="stat-item">
                    <div>Consecutive Errors</div>
                    <div class="stat-value">{connection_health['consecutive_errors']}</div>
                </div>
            </div>
            
            <div class="links">
                <a href="/api/stats">📊 Detailed Stats</a>
                <a href="/api/health">🏥 Health Check</a>
                <a href="/api/stocks">📈 Stock List</a>
            </div>
            
            <div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 5px; font-size: 12px;">
                <strong>Last Updated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
                <strong>Auto-refresh:</strong> 30 seconds
            </div>
        </div>
    </body>
    </html>
    """

# Graceful shutdown handler
import atexit
import signal

def cleanup():
    """Cleanup function for graceful shutdown"""
    logger.info("Shutting down gracefully...")
    try:
        flush_db_batch()  # Ensure all data is written before shutdown
        connection_health['is_connected'] = False
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    cleanup()
    exit(0)

# Register cleanup handlers
atexit.register(cleanup)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Error handlers for Flask
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(429)
def rate_limit_error(error):
    return jsonify({"error": "Rate limit exceeded"}), 429

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    logger.info(f"Configured for {stats['current_instruments']} instruments with rate limiting")
    
    # Use appropriate settings for Render
    port = int(os.environ.get('PORT', 5000))
    
    if ENVIRONMENT == 'production':
        # Production settings for Render
        app.run(
            debug=False,
            host='0.0.0.0',
            port=port,
            threaded=True,
            use_reloader=False
        )
    else:
        # Development settings
        app.run(
            debug=True,
            host='0.0.0.0',
            port=port
        )
