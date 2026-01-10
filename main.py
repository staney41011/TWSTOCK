import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- 設定參數 ---
LOOKBACK_LONG = 500  # 林則行: 兩年新高
MA_SHORT = 60        # 季線 (出場判斷用)
VOL_MA = 20          # 成交量均線
DATA_FILE = "data.json"

def get_tw_stock_list():
    """取得台灣上市櫃股票代號"""
    twse = twstock.twse
    tpex = twstock.tpex
    stocks = []
    for code in twse:
        if len(code) == 4: stocks.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4: stocks.append(f"{code}.TWO")
    return stocks

def analyze_stock(ticker, check_exit=False):
    """
    check_exit=False -> 掃描買進 (林則行策略)
    check_exit=True  -> 掃描賣出 (跌破季線)
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="2y")
        
        if len(df) < 250: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        ma60 = df['Close'].rolling(window=MA_SHORT).mean()
        curr_ma60 = ma60.iloc[-1]

        # --- 賣出檢查模式 ---
        if check_exit:
            # 如果收盤價跌破季線，且昨天還在季線上 (剛跌破)
            # 或者單純檢查現在是否低於季線
            is_below_ma60 = latest['Close'] < curr_ma60
            if is_below_ma60:
                return {
                    "code": ticker,
                    "name": ticker, # yfinance 抓中文名較慢，先用代號
                    "price": float(f"{latest['Close']:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": "跌破季線 (60MA)"
                }
            return None

        # --- 買進檢查模式 (林則行) ---
        if latest['Volume'] < 500000: return None # 濾掉無量

        lookback_days = min(len(df)-1, LOOKBACK_LONG)
        window_high = df['Close'][-lookback_days:-1].max()
        
        is_breaking_high = latest['Close'] > window_high
        is_ma60_up = curr_ma60 > ma60.iloc[-2]
        is_above_ma60 = latest['Close'] > curr_ma60
        
        vol_ma20 = df['Volume'].rolling(window=VOL_MA).mean().iloc[-1]
        is_volume_spike = latest['Volume'] > (vol_ma20 * 1.5)

        score = 0
        reasons = []
        if is_breaking_high: score += 2; reasons.append("突破兩年高")
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
    
    # 1. 讀取歷史資料 (為了追蹤持股)
    history_data = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
        except:
            pass

    # 找出過去 60 天內曾入選的股票代號，作為「潛在持倉」來檢查是否出場
    potential_holdings = set()
    for day_record in history_data[-60:]: # 只看最近兩個月入選的
        for stock in day_record.get('buy', []):
            potential_holdings.add(stock['code'])
            
    all_stocks = get_tw_stock_list()
    # 測試時限制數量，正式跑請拿掉下一行
    # all_stocks = all_stocks[:50] 
    
    today_buys = []
    today_exits = []

    # 2. 執行買入掃描
    print(f"正在掃描全市場買點 ({len(all_stocks)} 檔)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, code, False) for code in all_stocks]
        for future in futures:
            res = future.result()
            if res: today_buys.append(res)

    # 3. 執行賣出掃描 (針對潛在持倉)
    print(f"正在檢查出場訊號 ({len(potential_holdings)} 檔)...")
    if potential_holdings:
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(analyze_stock, code, True) for code in list(potential_holdings)]
            for future in futures:
                res = future.result()
                if res: today_exits.append(res)

    # 排序
    today_buys.sort(key=lambda x: (-x['score'], -x['price']))
    
    # 4. 存檔
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # 檢查今天是否已經跑過 (避免重複)，若跑過則更新，沒跑過則新增
    new_record = {
        "date": today_str,
        "buy": today_buys,
        "sell": today_exits
    }
    
    # 簡單邏輯：如果最後一筆是今天的，就覆蓋；否則 append
    if history_data and history_data[-1]['date'] == today_str:
        history_data[-1] = new_record
    else:
        history_data.append(new_record)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=2)

    print(f"掃描完成！資料已更新至 {DATA_FILE}")

if __name__ == "__main__":
    main()
