import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- 策略參數 ---
LOOKBACK_LONG = 500    # 2年新高
MA_W13 = 65            # 13週線
STOP_LOSS_PCT = 0.08   # 停損 8%

# 基本面門檻
MIN_ROE = 0.08         # ROE 至少 8%
MAX_PE = 60            # 本益比濾網 (超過60視為太貴，僅供參考不強制剔除)

DATA_FILE = "data.json"

def get_tw_stock_list():
    twse = twstock.twse
    tpex = twstock.tpex
    stocks = []
    # 過濾：只抓長度4碼的普通股
    for code in twse:
        if len(code) == 4: stocks.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4: stocks.append(f"{code}.TWO")
    return stocks

def get_fundamentals(stock_obj):
    """
    抓取基本面資料 (這會花費額外時間，只針對技術面過關的股票執行)
    """
    try:
        info = stock_obj.info
        
        # 抓取本益比 (如果虧損會是 None)
        pe = info.get('trailingPE')
        if pe is None: pe = 999 # 虧損或無資料
        
        # 抓取獲利成長率 (Quarterly Earnings Growth YoY)
        growth = info.get('earningsGrowth') # 例如 0.25 代表 25%
        
        # 抓取 ROE
        roe = info.get('returnOnEquity')
        
        return {
            "pe": pe,
            "growth": growth,
            "roe": roe
        }
    except:
        return {"pe": 999, "growth": None, "roe": None}

def analyze_stock(ticker, check_exit_stocks=None):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="3y") 
        
        if len(df) < LOOKBACK_LONG + 10: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        ma65 = df['Close'].rolling(window=MA_W13).mean()
        curr_ma65 = ma65.iloc[-1]
        prev_ma65 = ma65.iloc[-2]

        # --- [模式 B] 掃描出場 ---
        if check_exit_stocks and ticker in check_exit_stocks:
            buy_price = check_exit_stocks[ticker]
            current_price = latest['Close']
            
            # 停損
            loss_pct = (current_price - buy_price) / buy_price
            if loss_pct <= -STOP_LOSS_PCT:
                return {
                    "code": ticker,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"觸發停損 (虧損 {loss_pct*100:.1f}%)"
                }

            # 趨勢出場
            is_ma_flattening = curr_ma65 <= prev_ma65
            if current_price < curr_ma65 and is_ma_flattening:
                return {
                    "code": ticker,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": "跌破13週線 且 趨勢轉弱"
                }
            return None

        # --- [模式 A] 掃描進場 ---
        if check_exit_stocks is not None: return None
        if latest['Volume'] < 500000: return None 

        # 1. 技術面篩選 (New High)
        window_high = df['Close'][-LOOKBACK_LONG-1:-1].max()
        is_new_high = latest['Close'] > window_high
        
        was_high_yesterday = prev['Close'] > window_high
        is_fresh_breakout = is_new_high and (not was_high_yesterday)

        open_price = latest['Open']
        is_big_candle = (latest['Close'] - open_price) / open_price > 0.015

        # 只有技術面通過，才去抓基本面 (節省時間與流量)
        if is_fresh_breakout and is_big_candle:
            
            # 2. 基本面篩選 (Fundamentals)
            fund_data = get_fundamentals(stock)
            
            # 評分系統
            score = 0
            reasons = ["突破2年新高", "實體紅K"]
            
            # 檢查成長率
            if fund_data['growth'] is not None and fund_data['growth'] > 0.15:
                score += 1
                reasons.append(f"獲利成長猛 ({fund_data['growth']*100:.0f}%)")
            elif fund_data['growth'] is not None and fund_data['growth'] > 0:
                reasons.append("獲利正成長")
            else:
                reasons.append("⚠️獲利衰退或無資料")

            # 檢查本益比
            pe_val = fund_data['pe']
            if pe_val < 25:
                score += 1
                reasons.append(f"本益比低 ({pe_val:.1f})")
            elif pe_val > MAX_PE:
                reasons.append(f"⚠️本益比過高 ({pe_val:.1f})")
            else:
                reasons.append(f"本益比合理 ({pe_val:.1f})")

            # 檢查 ROE
            if fund_data['roe'] is not None and fund_data['roe'] > MIN_ROE:
                score += 1
                reasons.append(f"ROE優 ({fund_data['roe']*100:.1f}%)")

            # 成交量加分
            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * 1.5:
                score += 1
                reasons.append("成交量爆發")

            # 總分過低代表僅有炒作，無基本面支撐，可選擇是否過濾
            # 這裡我們先全列出來，讓使用者自己看基本面標籤
            
            return {
                "code": ticker,
                "name": ticker, 
                "price": float(f"{latest['Close']:.2f}"),
                "score": score, # 這裡的分數變成 綜合評分
                "reasons": reasons,
                "fundamentals": {
                    "pe": "N/A" if pe_val==999 else f"{pe_val:.1f}",
                    "growth": "N/A" if fund_data['growth'] is None else f"{fund_data['growth']*100:.1f}%",
                    "roe": "N/A" if fund_data['roe'] is None else f"{fund_data['roe']*100:.1f}%"
                },
                "date": latest.name.strftime('%Y-%m-%d')
            }
        return None

    except Exception:
        return None

def main():
    print("啟動林則行策略 (基本面加強版) 掃描...")
    
    history_data = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
        except:
            history_data = []

    holdings_map = {} 
    if isinstance(history_data, list):
        for day_record in history_data[-60:]: 
            for stock in day_record.get('buy', []):
                if stock['code'] not in holdings_map:
                    holdings_map[stock['code']] = stock['price']
            
    all_stocks = get_tw_stock_list()
    # all_stocks = all_stocks[:50] # 測試用
    
    today_buys = []
    today_exits = []

    print(f"正在掃描買點 ({len(all_stocks)} 檔)...")
    # 因為抓基本面比較慢，這裡維持多執行緒
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, code, None) for code in all_stocks]
        for future in futures:
            res = future.result()
            if res: today_buys.append(res)

    print(f"正在檢查持倉出場 ({len(holdings_map)} 檔)...")
    if holdings_map:
        check_list = list(holdings_map.keys())
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(analyze_stock, code, holdings_map) for code in check_list]
            for future in futures:
                res = future.result()
                if res: today_exits.append(res)

    today_buys.sort(key=lambda x: -x['score']) # 改用分數排序 (基本面好的排前面)
    
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
