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
API_BATCH_SIZE = 5
BATCH_INTERVAL_SEC = 5
RESET_TIME = "09:15"
STOCK_LIST_FILE = "stock_list.csv"
DB_FILE = "orderflow_data.db"

# --- LOGGING SETUP ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if ENVIRONMENT == 'production':
    logging.disable(logging.DEBUG)

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)
app.config['DEBUG'] = ENVIRONMENT != 'production'

# Global variables
analyzer = None
delta_history = defaultdict(lambda: defaultdict(lambda: {'buy': 0, 'sell': 0}))
last_reset_date = [None]
prev_ltp = defaultdict(lambda: None)
prev_volume = defaultdict(lambda: 0)
prev_oi = defaultdict(lambda: 0)  # Track previous OI

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
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1NTE1NDY2LCJpYXQiOjE3NjU0MjkwNjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwMjQ0MjY4In0.wD3K7JjlDlT28UaHh_hWFtncemKKHIqKekSlfJrDR3h9gDdCTl3vbUADcVfeMqgNwL3JEgOCC9vFXee0CDdkOQ"
analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)

def get_instrument_list():
    stocks = []
    try:
        with open(STOCK_LIST_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                exch = row.get("exchange")
                seg = row.get("segment")
                sec_id = str(row["security_id"])
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
                    tick_delta REAL,
                    open_interest REAL,
                    oi_change REAL,
                    oi_interpretation TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_security_timestamp ON orderflow(security_id, timestamp)')
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        stats['errors'] += 1

init_db()

def calculate_oi_interpretation(price_change, oi_change):
    """
    Calculate OI interpretation based on price and OI changes
    
    Returns: Long Buildup, Short Buildup, Long Unwinding, Short Covering, or Neutral
    """
    if oi_change > 0:
        if price_change > 0:
            return "Long Buildup"  # Price ↑, OI ↑
        elif price_change < 0:
            return "Short Buildup"  # Price ↓, OI ↑
        else:
            return "Neutral"  # No price change
    elif oi_change < 0:
        if price_change > 0:
            return "Short Covering"  # Price ↑, OI ↓
        elif price_change < 0:
            return "Long Unwinding"  # Price ↓, OI ↓
        else:
            return "Neutral"  # No price change
    else:
        return "Neutral"  # No OI change

def store_in_db_batch(batch_data):
    """Store multiple records in database at once"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.executemany(
                "INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume, buy_initiated, sell_initiated, tick_delta, open_interest, oi_change, oi_interpretation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_data
            )
        stats['db_writes'] += len(batch_data)
        logger.debug(f"Batch inserted {len(batch_data)} records")
    except Exception as e:
        logger.error(f"Database batch insert failed: {e}")
        stats['errors'] += 1

def add_to_db_batch(security_id, timestamp, buy, sell, ltp, volume, buy_initiated=None, sell_initiated=None, tick_delta=None, open_interest=None, oi_change=None, oi_interpretation=None):
    """Add record to batch for later database insertion"""
    global db_batch
    
    with db_batch_lock:
        db_batch.append((security_id, timestamp, buy, sell, ltp, volume, buy_initiated, sell_initiated, tick_delta, open_interest, oi_change, oi_interpretation))
        
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

                    stats['messages_processed'] += 1
                    
                    if stats['messages_processed'] % 1000 == 0:
                        logger.info(f"Processed {stats['messages_processed']} messages, {stats['errors']} errors, {stats['db_writes']} DB writes")

                    live_market_data[security_id] = response
                    
                    if security_id not in orderflow_history:
                        orderflow_history[security_id] = []
                    orderflow_history[security_id].append(response)
                    
                    if len(orderflow_history[security_id]) > 10000:
                        orderflow_history[security_id] = orderflow_history[security_id][-5000:]
                    
                    msg_type = response.get("type", "")
                    if msg_type in ("Quote Data", "Quote"):
                        try:
                            ltt = response.get("LTT")
                            today = datetime.now(IST).strftime('%Y-%m-%d')
                            timestamp = f"{today} {ltt}" if ltt else datetime.now(IST).isoformat()
                            buy = response.get("total_buy_quantity", 0)
                            sell = response.get("total_sell_quantity", 0)
                            ltp_val = float(response.get("LTP", 0))
                            volume = response.get("volume", 0)
                            
                            # Get Open Interest
                            current_oi = response.get("OI", 0) or response.get("open_interest", 0)
                            previous_oi = prev_oi.get(security_id, 0)
                            oi_change = current_oi - previous_oi if previous_oi > 0 else 0
                            
                            # Calculate price change for OI interpretation
                            prev = prev_ltp[security_id]
                            price_change = (ltp_val - prev) if prev is not None else 0
                            
                            # Get OI interpretation
                            oi_interpretation = calculate_oi_interpretation(price_change, oi_change)
                            
                            # Update previous OI
                            prev_oi[security_id] = current_oi
                            
                            # Tick-rule logic with volume de-duplication
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

                            # Add to batch with OI data
                            add_to_db_batch(security_id, timestamp, buy, sell, ltp_val, volume, 
                                          buy_initiated, sell_initiated, tick_delta, 
                                          current_oi, oi_change, oi_interpretation)
                            
                        except Exception as e:
                            logger.error(f"Error processing quote data for {security_id}: {e}")
                            stats['errors'] += 1
                
                time.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Marketfeed thread crashed: {e}. Restarting in 5 seconds...")
            stats['errors'] += 1
            time.sleep(5)

marketfeed_thread_obj = threading.Thread(target=marketfeed_thread, daemon=True)
marketfeed_thread_obj.start()

def batch_flush_thread():
    while True:
        time.sleep(30)
        flush_db_batch()

threading.Thread(target=batch_flush_thread, daemon=True).start()

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
        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM orderflow WHERE security_id = ? ORDER BY timestamp ASC",
                conn, params=(security_id,)
            )
        
        if df.empty:
            return jsonify([])
        
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
                
                buy_initiated_sum = group['buy_initiated'].sum()
                sell_initiated_sum = group['sell_initiated'].sum()
                tick_delta_sum = group['tick_delta'].sum()
                
                # OI aggregation
                oi_change_sum = group['oi_change'].sum() if 'oi_change' in group.columns else 0
                current_oi = end['open_interest'] if 'open_interest' in end and pd.notnull(end['open_interest']) else 0
                
                # Get most common OI interpretation in the interval
                if 'oi_interpretation' in group.columns:
                    oi_interp = group['oi_interpretation'].mode()[0] if not group['oi_interpretation'].mode().empty else 'Neutral'
                else:
                    oi_interp = 'Neutral'
                
                threshold = 0
                if tick_delta_sum > threshold:
                    inference = "Buy Dominant"
                elif tick_delta_sum < -threshold:
                    inference = "Sell Dominant"
                else:
                    inference = "Neutral"
                
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
                    'close': close_,
                    'open_interest': current_oi,
                    'oi_change': oi_change_sum,
                    'oi_interpretation': oi_interp
                })
            elif len(group) == 1:
                row = group.iloc[0]
                ohlc = row['ltp'] if pd.notnull(row['ltp']) else None
                current_oi = row['open_interest'] if 'open_interest' in row and pd.notnull(row['open_interest']) else 0
                oi_change = row['oi_change'] if 'oi_change' in row and pd.notnull(row['oi_change']) else 0
                oi_interp = row['oi_interpretation'] if 'oi_interpretation' in row else 'Neutral'
                
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
                    'close': ohlc,
                    'open_interest': current_oi,
                    'oi_change': oi_change,
                    'oi_interpretation': oi_interp
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

        today = datetime.now(IST).date()
        df = df[df['timestamp'].dt.date == today]

        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        tick_deltas = [group['tick_delta'].sum() for _, group in grouped if not group.empty]

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

import atexit

def cleanup():
    logger.info("Shutting down gracefully...")
    flush_db_batch()

atexit.register(cleanup)

if __name__ == '__main__':
    logger.info(f"Starting Flask server in {ENVIRONMENT} mode with log level {LOG_LEVEL}")
    app.run(debug=app.config['DEBUG'], host='0.0.0.0', port=5000)
