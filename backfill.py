import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- 設定 ---
DATA_DIR = "data"
OUTPUT_FILE = "data.json"

# --- NaN 防護 ---
def clean_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj

# --- 策略邏輯 (必須與 main.py 一致) ---
def strategy_low_volatility(df):
    if len(df) < 205: return None
    
    close_series = df['Close']
    vol_series = df['Volume']
    high_series = df['High']
    low_series = df['Low']
    
    ma50 = close_series.rolling(window=50, min_periods=40).mean()
    ma200 = close_series.rolling(window=200, min_periods=150).mean()
    vol_ma50 = vol_series.rolling(window=50, min_periods=40).mean()
    std_10 = close_series.rolling(window=10, min_periods=5).std().iloc[-1]
    
    curr_close = close_series.iloc[-1]
    curr_open = df['Open'].iloc[-1]
    curr_low = low_series.iloc[-1]
    curr_vol = vol_series.iloc[-1]
    curr_ma50 = ma50.iloc[-1]
    curr_ma200 = ma200.iloc[-1]
    curr_vol_ma50 = vol_ma50.iloc[-1]
    prev_high = high_series.iloc[-2]

    if pd.isna(curr_ma50) or pd.isna(curr_ma200): return None

    # Core
    cond_trend = (curr_close > curr_ma200) and (curr_ma50 > curr_ma200)
    cond_support = (curr_close > curr_ma50)
    
    if not (cond_trend and cond_support): return None

    # Signals
    signals = []
    body_size = abs(curr_close - curr_open)
    is_doji = body_size < (curr_close * 0.005)
    is_low_vol = pd.notna(curr_vol_ma50) and (curr_vol < curr_vol_ma50 * 0.6)
    if is_doji and is_low_vol: signals.append("★ 縮量十字星")

    if curr_low > prev_high: signals.append("★ 強力跳空")

    dist_to_ma50 = (curr_close - curr_ma50) / curr_ma50
    if 0 <= dist_to_ma50 < 0.03: signals.append("★ 50MA 完美回測")

    tag = "OBSERVE"
    desc_text = "趨勢多頭 (觀察中)"
    if signals:
        tag = "META"
        desc_text = " | ".join(signals)
        
    vol_pct = 0
    if pd.notna(std_10) and curr_close > 0:
        vol_pct = round((std_10 / curr_close) * 100, 2)

    return {
        "tag": tag,
        "volatility_pct": vol_pct,
        "trend_status": "多頭排列",
        "volume_status": "量能收縮" if (pd.notna(curr_vol_ma50) and curr_vol < curr_vol_ma50) else "量能放大",
        "desc": desc_text
    }

# --- 輔助函式 ---
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

def fetch_and_process(ticker, target_date_str):
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        end_date = target_date + timedelta(days=1)
        start_date = target_date - timedelta(days=500)
        
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if df.empty or len(df) < 200: return None
        
        # 確保最後一天是目標日期 (若當天沒交易，回傳 None)
        last_date = df.index[-1].strftime("%Y-%m-%d")
        if last_date != target_date_str: return None

        res = strategy_low_volatility(df)
        
        if res:
            latest = df.iloc[-1]
            return {
                "code": ticker,
                "name": get_stock_name(ticker),
                "region": "TW",
                "price": float(f"{latest['Close'].iloc[0] if isinstance(latest['Close'], pd.Series) else latest['Close']:.2f}"),
                **res
            }
    except: return None
    return None

def main():
    print("⏳ 啟動時光機：補跑厚積薄發 V2...")
    
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    stock_list = get_tw_stock_list()
    
    for file_path in files:
        target_date_str = os.path.basename(file_path).replace(".json", "")
        print(f"\n📅 正在回測日期: {target_date_str} ...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            record = json.load(f)
            
        new_low_vol_list = []
        
        with ThreadPoolExecutor(max_workers=10) as exc:
            futures = [exc.submit(fetch_and_process, s, target_date_str) for s in stock_list]
            for f in as_completed(futures):
                res = f.result()
                if res: new_low_vol_list.append(res)
        
        # 清洗 NaN 後寫入
        new_low_vol_list.sort(key=lambda x: x['volatility_pct'])
        if "strategies" not in record: record["strategies"] = {}
        record["strategies"]["low_volatility"] = clean_for_json(new_low_vol_list)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            
        print(f"✅ {target_date_str} 更新完成，找到 {len(new_low_vol_list)} 檔。")

    # 重建總檔
    print("\n📦 重建 data.json...")
    final_history = []
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                final_history.append(json.load(f))
        except: pass
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(clean_for_json(final_history), f, ensure_ascii=False, indent=2)
        
    print("🎉 全部完成！")

if __name__ == "__main__":
    main()
