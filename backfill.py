import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import math
import time
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

# --- 策略邏輯 (Trend V2) ---
def strategy_low_volatility(df):
    # 資料長度檢查
    if len(df) < 205: return None
    
    close_series = df['Close']
    vol_series = df['Volume']
    high_series = df['High']
    low_series = df['Low']
    
    # 均線計算 (與 main.py 一致)
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

    # 核心條件
    cond_trend = (curr_close > curr_ma200) and (curr_ma50 > curr_ma200)
    cond_support = (curr_close > curr_ma50)
    
    if not (cond_trend and cond_support): return None

    # 訊號偵測
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
    print("🐢 啟動穩定版回補程序 (單線程，請耐心等候)...")
    
    # 只針對 1/16 之後的檔案進行回補 (節省時間)
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    target_files = [f for f in files if "2026-01-16" in f] # 鎖定 1/16
    
    if not target_files:
        print("找不到 2026-01-16 的檔案，請先確認檔案存在")
        return

    stock_list = get_tw_stock_list()
    # stock_list = stock_list[:50] # debug 用，只跑前50檔，正式跑請註解掉這行
    
    for file_path in target_files:
        target_date_str = os.path.basename(file_path).replace(".json", "")
        print(f"\n📅 正在修復日期: {target_date_str} (處理中...)")
        
        # 讀取原本的檔案內容
        with open(file_path, 'r', encoding='utf-8') as f:
            record = json.load(f)
            
        new_low_vol_list = []
        
        # 單線程迴圈 (穩定度 MAX)
        for i, ticker in enumerate(stock_list):
            if i % 100 == 0: print(f"   進度: {i}/{len(stock_list)}...")
            
            try:
                # 使用與 main.py 一致的 yf.Ticker 方法
                stock = yf.Ticker(ticker)
                # 抓取 3 年資料，確保有足夠的歷史數據算 MA200
                # 注意：這裡不切分 end date，直接抓最新，然後取 iloc[-1]
                # (因為我們是在補跑過去幾天的資料，假設該日已收盤)
                df = stock.history(period="1y") 
                
                if df.empty or len(df) < 205: continue
                
                # 簡單確認日期：如果是補跑 1/16，我們確保資料最後一筆日期 <= 1/16
                # 這裡做一個簡單的切割，把 1/16 之後的資料切掉，模擬當天的狀況
                df = df[df.index.strftime('%Y-%m-%d') <= target_date_str]
                
                if df.empty: continue
                
                # 再次確認切完後的最後一天是不是目標日期
                last_date = df.index[-1].strftime("%Y-%m-%d")
                if last_date != target_date_str: continue

                res = strategy_low_volatility(df)
                
                if res:
                    latest = df.iloc[-1]
                    s_data = {
                        "code": ticker,
                        "name": get_stock_name(ticker),
                        "region": "TW",
                        "price": float(f"{latest['Close']:.2f}"),
                        **res
                    }
                    new_low_vol_list.append(s_data)
                    
                    # 🔍 監控華邦電
                    if "2344" in ticker:
                        print(f"   🔥 抓到了！華邦電已入列 (Tag: {s_data['tag']})")

            except Exception as e:
                # print(f"Error {ticker}: {e}")
                pass
        
        # 排序並存檔
        new_low_vol_list.sort(key=lambda x: x['volatility_pct'])
        if "strategies" not in record: record["strategies"] = {}
        record["strategies"]["low_volatility"] = clean_for_json(new_low_vol_list)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            
        print(f"✅ {target_date_str} 更新完成，共找到 {len(new_low_vol_list)} 檔厚積薄發股。")

    # 重建 data.json
    print("📦 重建總索引 data.json...")
    final_history = []
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                final_history.append(json.load(f))
        except: pass
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(clean_for_json(final_history), f, ensure_ascii=False, indent=2)
        
    print("🎉 修復作業結束！")

if __name__ == "__main__":
    main()
