import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import math
import time
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

def get_market_ret_at_date(target_date_str):
    try:
        market = yf.Ticker("0050.TW")
        target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
        end_dt = target_dt + timedelta(days=5)
        start_dt = target_dt - timedelta(days=60)
        df = market.history(start=start_dt, end=end_dt)
        if target_date_str in df.index:
            target_idx = df.index.get_loc(target_date_str)
            if target_idx >= 20:
                latest = df.iloc[target_idx]['Close']
                past_20 = df.iloc[target_idx-20]['Close']
                return (latest - past_20) / past_20
    except: pass
    return None

# V5 融合版策略 (Backfill版)
def strategy_granville_vcp(df, market_ret_20d):
    if len(df) < 205: return None
    close_s = df['Close']; vol_s = df['Volume']
    ma200 = close_s.rolling(window=200, min_periods=150).mean()
    curr_ma200 = float(ma200.iloc[-1]); prev_ma200 = float(ma200.iloc[-2])
    ma20 = close_s.rolling(window=20, min_periods=15).mean()
    vol_ma50 = vol_s.rolling(window=50, min_periods=40).mean()
    std_20 = close_s.rolling(window=20, min_periods=15).std()
    curr_close = float(close_s.iloc[-1]); prev_close = float(close_s.iloc[-2])
    curr_vol = float(vol_s.iloc[-1]); curr_ma20 = float(ma20.iloc[-1])
    curr_vol_ma50 = float(vol_ma50.iloc[-1]); curr_std_20 = float(std_20.iloc[-1])

    if pd.isna(curr_ma200): return None
    
    # Step 1: Granville Filter
    granville_type = None
    if curr_ma200 <= prev_ma200: return None # 趨勢必須向上
    
    if prev_close < prev_ma200 and curr_close > curr_ma200:
        granville_type = "法則二 (假跌破)"
    elif curr_close > curr_ma200:
        dist = (df['Low'].iloc[-1] - curr_ma200) / curr_ma200
        if 0 <= dist < 0.015 and curr_close > df['Open'].iloc[-1]:
             granville_type = "法則三 (回測支撐)"
    
    if not granville_type: return None

    # Step 2: Scoring
    score = 0; signals = []
    if pd.notna(curr_std_20) and curr_ma20 > 0:
        if (4 * curr_std_20) / curr_ma20 < 0.10: score += 1; signals.append("布林壓縮")
    if pd.notna(curr_vol_ma50) and curr_vol_ma50 > 0:
        if curr_vol < (curr_vol_ma50 * 0.5): score += 1; signals.append("量能急凍")
    if market_ret_20d is not None and len(close_s) > 22:
        price_20_ago = float(close_s.iloc[-21])
        if price_20_ago > 0:
            if (curr_close - price_20_ago) / price_20_ago > market_ret_20d: score += 1; signals.append("相對強勢")

    tag = f"★ {score}分"
    if score == 0: tag = "觀察 (0分)"
    if score == 3: tag = "★ 3分 (滿分)"
    
    desc_text = f"【{granville_type}】"
    if signals: desc_text += " + " + " | ".join(signals)
    else: desc_text += " (符合葛蘭碧買點)"

    vol_pct = 0
    std_10 = close_s.rolling(window=10, min_periods=5).std().iloc[-1]
    if pd.notna(std_10) and curr_close > 0: vol_pct = round((std_10 / curr_close) * 100, 2)

    return {
        "tag": tag, "volatility_pct": vol_pct, "trend_status": f"MA200上揚 ({granville_type})",
        "volume_status": "量能收縮" if (pd.notna(curr_vol_ma50) and curr_vol < curr_vol_ma50) else "量能放大",
        "desc": desc_text, "score_val": score
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
    print("🐢 啟動 V5 回補 (葛蘭碧 X 厚積薄發)...")
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    target_files = [f for f in files if "2026-01-16" in f] 
    if not target_files: return
    stock_list = get_tw_stock_list()
    
    for file_path in target_files:
        target_date_str = os.path.basename(file_path).replace(".json", "")
        print(f"\n📅 修復: {target_date_str}")
        market_ret = get_market_ret_at_date(target_date_str)
        
        with open(file_path, 'r', encoding='utf-8') as f: record = json.load(f)
        new_list = []
        for i, ticker in enumerate(stock_list):
            if i % 100 == 0: print(f"   {i}/{len(stock_list)}...")
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period="1y") 
                if df.empty or len(df) < 205: continue
                df = df[df.index.strftime('%Y-%m-%d') <= target_date_str]
                if df.empty: continue
                if df.index[-1].strftime("%Y-%m-%d") != target_date_str: continue
                
                res = strategy_granville_vcp(df, market_ret)
                if res:
                    latest = df.iloc[-1]
                    new_list.append({
                        "code": ticker, "name": get_stock_name(ticker), "region": "TW",
                        "price": float(f"{latest['Close']:.2f}"), **res
                    })
            except: pass
        
        new_list.sort(key=lambda x: -x.get('score_val', 0))
        if "strategies" not in record: record["strategies"] = {}
        record["strategies"]["low_volatility"] = clean_for_json(new_list)
        
        with open(file_path, 'w', encoding='utf-8') as f: json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"✅ 完成，找到 {len(new_list)} 檔。")

    final_history = []
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f: final_history.append(json.load(f))
        except: pass
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f: json.dump(clean_for_json(final_history), f, ensure_ascii=False, indent=2)
    print("🎉 Done!")

if __name__ == "__main__":
    main()
