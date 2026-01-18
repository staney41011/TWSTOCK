import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

DATA_DIR = "data"
OUTPUT_FILE = "data.json"

def clean_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj

# --- 抓取指定日期的大盤歷史 RS ---
def get_market_ret_at_date(target_date_str):
    try:
        market = yf.Ticker("0050.TW")
        # 抓取包含目標日期的區間
        target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
        end_dt = target_dt + timedelta(days=5)
        start_dt = target_dt - timedelta(days=60)
        
        df = market.history(start=start_dt, end=end_dt)
        
        # 找到目標日期的收盤價
        if target_date_str in df.index:
            target_idx = df.index.get_loc(target_date_str)
            if target_idx >= 20:
                latest = df.iloc[target_idx]['Close']
                past_20 = df.iloc[target_idx-20]['Close']
                return (latest - past_20) / past_20
    except: pass
    return None

def strategy_low_volatility(df, market_ret_20d):
    if len(df) < 205: return None
    
    close_s = df['Close']; vol_s = df['Volume']
    ma20 = close_s.rolling(window=20, min_periods=15).mean()
    ma50 = close_s.rolling(window=50, min_periods=40).mean()
    ma200 = close_s.rolling(window=200, min_periods=150).mean()
    vol_ma50 = vol_s.rolling(window=50, min_periods=40).mean()
    std_20 = close_s.rolling(window=20, min_periods=15).std()
    
    curr_close = float(close_s.iloc[-1])
    curr_vol = float(vol_s.iloc[-1])
    curr_ma20 = float(ma20.iloc[-1])
    curr_ma50 = float(ma50.iloc[-1])
    curr_ma200 = float(ma200.iloc[-1])
    curr_vol_ma50 = float(vol_ma50.iloc[-1])
    curr_std_20 = float(std_20.iloc[-1])

    if pd.isna(curr_ma50) or pd.isna(curr_ma200): return None

    # Core
    cond_trend = (curr_close > curr_ma200) and (curr_ma50 > curr_ma200)
    cond_support = (curr_close > curr_ma50)
    if not (cond_trend and cond_support): return None

    # Scoring
    score = 0
    signals = []

    # 1. BB Squeeze
    if pd.notna(curr_std_20) and curr_ma20 > 0:
        bb_width = (4 * curr_std_20) / curr_ma20
        if bb_width < 0.10: score += 1; signals.append("布林壓縮")

    # 2. Vol Dry-up
    if pd.notna(curr_vol_ma50) and curr_vol_ma50 > 0:
        if curr_vol < (curr_vol_ma50 * 0.5): score += 1; signals.append("量能急凍")

    # 3. RS
    if market_ret_20d is not None and len(close_s) > 22:
        price_20_ago = float(close_s.iloc[-21])
        if price_20_ago > 0:
            stock_ret_20d = (curr_close - price_20_ago) / price_20_ago
            if stock_ret_20d > market_ret_20d: score += 1; signals.append("相對強勢")

    if score == 0: return None

    tag = f"★ {score}分"
    if score == 3: tag = "★ 3分 (滿分)"
    
    desc_text = " | ".join(signals)
    vol_pct = 0
    if pd.notna(curr_std_20) and curr_close > 0: vol_pct = round((curr_std_20 / curr_close) * 100, 2)

    return {
        "tag": tag,
        "volatility_pct": vol_pct,
        "trend_status": "多頭排列",
        "volume_status": "量能收縮" if (pd.notna(curr_vol_ma50) and curr_vol < curr_vol_ma50) else "量能放大",
        "desc": desc_text,
        "score_val": score
    }

def get_tw_stock_list():
    stocks = []
    for code in twstock.twse:
        if len(code) == 4: stocks.append(f"{code}.TW")
    for code in twstock.tpex:
        if len(code) == 4: stocks.append(f"{code}.TWO")
    return stocks

def get_stock_name(ticker):
    if ticker.endswith('.TW'):
        code = ticker.split('.')[0]
        if code in twstock.codes: return twstock.codes[code].name
    return ticker

def main():
    print("🐢 啟動 V4 回補程序 (含 RS 計算)...")
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    target_files = [f for f in files if "2026-01-16" in f] 
    
    if not target_files: return

    stock_list = get_tw_stock_list()
    
    for file_path in target_files:
        target_date_str = os.path.basename(file_path).replace(".json", "")
        print(f"\n📅 修復: {target_date_str} (計算大盤 RS...)")
        
        # 取得當日大盤 RS 基準
        market_ret = get_market_ret_at_date(target_date_str)
        if market_ret: print(f"   大盤 20日漲幅基準: {market_ret*100:.2f}%")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            record = json.load(f)
            
        new_low_vol_list = []
        
        # 單線程跑 (穩定優先)
        for i, ticker in enumerate(stock_list):
            if i % 100 == 0: print(f"   {i}/{len(stock_list)}...")
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period="1y") 
                if df.empty or len(df) < 205: continue
                
                df = df[df.index.strftime('%Y-%m-%d') <= target_date_str]
                if df.empty: continue
                
                last_date = df.index[-1].strftime("%Y-%m-%d")
                if last_date != target_date_str: continue

                res = strategy_low_volatility(df, market_ret)
                
                if res:
                    latest = df.iloc[-1]
                    new_low_vol_list.append({
                        "code": ticker,
                        "name": get_stock_name(ticker),
                        "region": "TW",
                        "price": float(f"{latest['Close']:.2f}"),
                        **res
                    })
            except: pass
        
        new_low_vol_list.sort(key=lambda x: -x.get('score_val', 0))
        if "strategies" not in record: record["strategies"] = {}
        record["strategies"]["low_volatility"] = clean_for_json(new_low_vol_list)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            
        print(f"✅ 完成，找到 {len(new_low_vol_list)} 檔。")

    final_history = []
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f: final_history.append(json.load(f))
        except: pass
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(clean_for_json(final_history), f, ensure_ascii=False, indent=2)
    print("🎉 Done!")

if __name__ == "__main__":
    main()
