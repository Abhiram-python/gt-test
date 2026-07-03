from flask import Flask, render_template
import asyncio
import websockets
import json
import requests
import time
import csv
import os
import numpy as np
from dataclasses import dataclass


app = Flask(__name__)

n=0


# ==========================================
# CONFIGURATION
# ==========================================
SYMBOL = "btcusdt"
WS_URL_DEPTH = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth@100ms"
WS_URL_TRADES = f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade"
REST_URL_DEPTH = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL.upper()}&limit=1000"

DATASET_FILE = "orderflow_dataset_v6t.csv" # Point to the new v3 dataset

BUCKET_INTERVAL_SEC = 10     
WARMUP_BUCKETS = 60           # Match the 60-bucket settings update

# REGRESSION TARGET: Maximum Favorable Excursion over 8 minutes.
TIME_BARRIER_SEC = 480       

# ==========================================
# DATA STRUCTURES
# ==========================================
@dataclass
class Trade:
    price: float
    qty: float
    is_buy: bool

class PendingFeatureRow:
    def __init__(self, timestamp, entry_price, features):
        self.timestamp = timestamp
        self.entry_price = entry_price
        self.features = features
        self.highest_price = entry_price
        self.lowest_price = entry_price

# ==========================================
# MAIN ENGINE
# ==========================================
class OrderFlowCollector:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.current_price = 0.0
        self.recent_trades = []
        
        self.buckets_processed = 0
        self.pending_rows = []
        self.saved_count = 0
        
        self.price_history = [] # NEW: Tracks the last 60 close prices for MA calculations
        
        self._init_csv()

    def _init_csv(self):
        os.makedirs(os.path.dirname(os.path.abspath(DATASET_FILE)), exist_ok=True)
        file_exists = os.path.isfile(DATASET_FILE)
        with open(DATASET_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                # ADDED the two new columns before target_return
                headers = [
                    'timestamp', 'close_price', 'delta', 'tps', 'volume', 
                    'obi', 'momentum_efficiency', 'absorption', 'evr', 
                    'price_to_ma_dist', 'rolling_volatility', 'target_return'
                ]
                writer.writerow(headers)

    def fetch_initial_snapshot(self):
        print("[SYSTEM] Fetching initial order book snapshot...")
        try:
            req=requests.get(REST_URL_DEPTH)
            print(req.status_code)
            print(req.text)
            res = req.json()
            for b in res['bids']: self.bids[float(b[0])] = float(b[1])
            for a in res['asks']: self.asks[float(a[0])] = float(a[1])
            print("[SYSTEM] Snapshot loaded successfully.")
        except Exception as e:
            print(f"[ERROR] Could not fetch snapshot: {e}")

    async def manage_depth_stream(self):
        async for ws in websockets.connect(WS_URL_DEPTH):
            try:
                async for msg in ws:
                    data = json.loads(msg)
                    for b in data['b']:
                        p, q = float(b[0]), float(b[1])
                        if q == 0: self.bids.pop(p, None)
                        else: self.bids[p] = q
                    for a in data['a']:
                        p, q = float(a[0]), float(a[1])
                        if q == 0: self.asks.pop(p, None)
                        else: self.asks[p] = q
            except websockets.exceptions.ConnectionClosed:
                continue

    async def manage_trade_stream(self):
        async for ws in websockets.connect(WS_URL_TRADES):
            try:
                async for msg in ws:
                    data = json.loads(msg)
                    price, qty, is_buyer_maker = float(data['p']), float(data['q']), data['m']
                    self.current_price = price
                    self.recent_trades.append(Trade(price, qty, not is_buyer_maker))
                    
                    # Update extreme prices for MFE tracking
                    for row in self.pending_rows:
                        if price > row.highest_price:
                            row.highest_price = price
                        elif price < row.lowest_price:
                            row.lowest_price = price
                            
                    self._check_time_horizon()
            except websockets.exceptions.ConnectionClosed:
                continue

    def _check_time_horizon(self):
        current_time = time.time()
        for i in range(len(self.pending_rows) - 1, -1, -1):
            row = self.pending_rows[i]
            
            # MAXIMUM FAVORABLE EXCURSION (MFE) Labeling
            if current_time - row.timestamp >= TIME_BARRIER_SEC:
                max_up_ret = (row.highest_price - row.entry_price) / row.entry_price
                max_down_ret = (row.lowest_price - row.entry_price) / row.entry_price
                
                # Label is the direction of the largest absolute spike during the 8 minutes
                if abs(max_up_ret) > abs(max_down_ret):
                    actual_return = max_up_ret
                else:
                    actual_return = max_down_ret
                    
                self._save_to_csv(row, actual_return)
                del self.pending_rows[i]

    def _save_to_csv(self, row, actual_return):
        with open(DATASET_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                row.timestamp, row.entry_price, f"{row.features['delta']:.4f}", f"{row.features['tps']:.2f}",
                f"{row.features['volume']:.4f}", f"{row.features['obi']:.4f}", f"{row.features['momentum_efficiency']:.8f}",
                f"{row.features['absorption']:.2f}", f"{row.features['evr']:.2f}", 
                f"{row.features['price_to_ma_dist']:.8f}", f"{row.features['rolling_volatility']:.8f}", # NEW
                f"{actual_return:.6f}"
            ])
        self.saved_count += 1
        print(f"[{time.strftime('%H:%M:%S')}] \033[92m[SAVED]\033[0m Target Return: {actual_return*100:+.3f}% | Dataset: {self.saved_count} rows")

    def _calculate_obi(self, depth=10):
        if not self.bids or not self.asks: return 0.0
        top_bids = sorted(self.bids.items(), key=lambda x: -x[0])[:depth]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        bid_vol = sum(q for p, q in top_bids)
        ask_vol = sum(q for p, q in top_asks)
        return (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-8)

    async def feature_extraction_loop(self):
        while True:
            await asyncio.sleep(BUCKET_INTERVAL_SEC)
            if not self.recent_trades or self.current_price == 0: continue

            trades = self.recent_trades.copy()
            self.recent_trades.clear()
            
            open_price, close_price = trades[0].price, trades[-1].price
            
            # Update price history for moving average
            self.price_history.append(close_price)
            if len(self.price_history) > WARMUP_BUCKETS:
                self.price_history.pop(0)
            
            self.buckets_processed += 1
            if self.buckets_processed < WARMUP_BUCKETS: 
                print(f"[{time.strftime('%H:%M:%S')}] Warming up Price Action math... ({self.buckets_processed}/{WARMUP_BUCKETS})")
                continue
                
            ret = (close_price - open_price) / open_price
            
            buy_vol = sum(t.qty for t in trades if t.is_buy)
            sell_vol = sum(t.qty for t in trades if not t.is_buy)
            delta, tps = buy_vol - sell_vol, len(trades) / BUCKET_INTERVAL_SEC
            
            abs_ret, abs_delta = abs(ret) + 1e-8, abs(delta) + 1e-8
            
            # Compute new live price action features
            moving_avg = np.mean(self.price_history)
            price_to_ma_dist = (close_price - moving_avg) / moving_avg
            rolling_volatility = np.std(self.price_history) / moving_avg
        
            # FIX: Properly indented to be inside the while loop
            features = {
                'delta': delta, 'tps': tps, 'volume': buy_vol + sell_vol,
                'obi': self._calculate_obi(10), 'momentum_efficiency': abs_ret / abs_delta,
                'absorption': abs_delta / abs_ret, 'evr': (abs_delta * tps) / abs_ret,
                'price_to_ma_dist': price_to_ma_dist, 'rolling_volatility': rolling_volatility # NEW
            }
            self.pending_rows.append(PendingFeatureRow(time.time(), close_price, features))
            
            print(f"[{time.strftime('%H:%M:%S')}] Vector Captured. Awaiting 8-min horizon. Pending: {len(self.pending_rows)} rows...")

    def force_resolve_pending(self):
        if not self.pending_rows: return
        print(f"\n[SYSTEM] Force resolving {len(self.pending_rows)} pending rows before shutdown...")
        for row in self.pending_rows:
            max_up_ret = (row.highest_price - row.entry_price) / row.entry_price
            max_down_ret = (row.lowest_price - row.entry_price) / row.entry_price
            actual_return = max_up_ret if abs(max_up_ret) > abs(max_down_ret) else max_down_ret
            self._save_to_csv(row, actual_return)

async def main():
    collector = OrderFlowCollector()
    collector.fetch_initial_snapshot()
    print(f"\n[SYSTEM] Starting Data Collection (Regression Target: 8 mins)...")
    tasks = [
        asyncio.create_task(collector.manage_depth_stream()),
        asyncio.create_task(collector.manage_trade_stream()),
        asyncio.create_task(collector.feature_extraction_loop())
    ]
    try: await asyncio.gather(*tasks)
    except asyncio.CancelledError: print("\n[SYSTEM] Shutting down streams gracefully...")
    finally: collector.force_resolve_pending()


import threading

def print_numbers():
   asyncio.run(main())


thread = threading.Thread(target=print_numbers)

thread.start()   # Starts the thread

print("Main thread finished.")


@app.route("/")
def home():
    global n

    # if __name__ == "__main__":
    # print("\n"*5,"yes is","\n"*5)
    # try: asyncio.run(main())
    # except KeyboardInterrupt: pass

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)