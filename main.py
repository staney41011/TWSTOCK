import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- 策略參數設定 (您可以在此調整) ---
LOOKBACK_LONG = 500  # 長期新高 (約2年)
MA_EXIT = 60         # 出場均線 (季線)
VOL_MA = 20          # 成交量均線
DATA_FILE = "data.json"

def get_tw_stock_list():
    twse = twstock.twse
    tpex = twstock.tpex
    stocks = []
    for code in twse:
        if len(code) == 4: stocks.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4: stocks.append(f"{code}.TWO")
    return stocks

def analyze_stock(ticker, check_exit=False):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="2y") # 抓取兩年資料
        
        if len(df) < 250: return None
        
        latest = df.iloc[-1]
        
        # 計算均線
        ma_exit_line = df['Close'].rolling(window=MA_EXIT).mean()
        curr_ma_exit = ma_exit_line.iloc[-1]

        # --- 賣出檢查模式 ---
        if check_exit:
            # 策略：收盤價 跌破 出場均線
            if latest['Close'] < curr_ma_exit:
                return {
                    "code": ticker,
                    "name": ticker,
                    "price": float(f"{latest['Close']:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"跌破 {MA_EXIT}MA"
                }
            return None

        # --- 買進檢查模式 ---
        if latest['Volume'] < 500000: return None 

        # 1. 創區間新高
        lookback_days = min(len(df)-1, LOOKBACK_LONG)
        window_high = df['Close'][-lookback_days:-1].max()
        is_breaking_high = latest['Close'] > window_high
        
        # 2. 均線趨勢
        # 使用 60MA 作為趨勢判斷
        ma60 = df['Close'].rolling(window=60).mean()
        curr_ma60 = ma60.iloc[-1]
        is_ma60_up = curr_ma60 > ma60.iloc[-2]
        is_above_ma60 = latest['Close'] > curr_ma60
        
        # 3. 成交量爆發
        vol_ma20 = df['Volume'].rolling(window=VOL_MA).mean().iloc[-1]
        is_volume_spike = latest['Volume'] > (vol_ma20 * 1.5)

        score = 0
        reasons = []
        if is_breaking_high: score += 2; reasons.append("突破長期新高")
        if is_ma60_up: score += 1; reasons.append("季線向上")
        if is_above_ma60: score += 1; reasons.append("站上季線")
        if is_volume_spike: score += 1; reasons.append("量增1.5倍")

        if score >= 4:
            return {
                "code": ticker,
                "name": ticker, 
                "price": float(f"{latest['Close']:.2f}"),
                "score": score,
                "reasons": reasons,
                "date": latest.name.strftime('%Y-%m-%d')
            }
        return None

    except Exception:
        return None

def main():
    print("啟動掃描...")
    
    history_data = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
        except:
            pass

    potential_holdings = set()
    if isinstance(history_data, list):
        for day_record in history_data[-60:]: 
            for stock in day_record.get('buy', []):
                potential_holdings.add(stock['code'])
            
    all_stocks = get_tw_stock_list()
    # all_stocks = all_stocks[:50] # 測試用
    
    today_buys = []
    today_exits = []

    print(f"正在掃描全市場買點 ({len(all_stocks)} 檔)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, code, False) for code in all_stocks]
        for future in futures:
            res = future.result()
            if res: today_buys.append(res)

    print(f"正在檢查出場訊號 ({len(potential_holdings)} 檔)...")
    if potential_holdings:
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(analyze_stock, code, True) for code in list(potential_holdings)]
            for future in futures:
                res = future.result()
                if res: today_exits.append(res)

    today_buys.sort(key=lambda x: (-x['score'], -x['price']))
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    new_record = {
        "date": today_str,
        "buy": today_buys,
        "sell": today_exits
    }
    
    if history_data and history_data[-1]['date'] == today_str:
        history_data[-1] = new_record
    else:
        history_data.append(new_record)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=2)

    print(f"掃描完成！資料已更新至 {DATA_FILE}")

if __name__ == "__main__":
    main()
