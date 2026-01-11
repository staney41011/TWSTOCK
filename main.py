import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta # <--- 新增 timedelta

# --- 策略參數 ---
LOOKBACK_LONG = 500    # 2年新高
MA_W13 = 65            # 13週線
STOP_LOSS_PCT = 0.08   # 停損 8%
MIN_ROE = 0.08         # ROE 8%
MAX_PE = 60            # 本益比

DATA_FILE = "data.json"

def get_tw_stock_list():
    """取得台灣上市櫃股票代號"""
    twse = twstock.twse
    tpex = twstock.tpex
    stocks = []
    for code in twse:
        if len(code) == 4: stocks.append({"code": f"{code}.TW", "region": "TW"})
    for code in tpex:
        if len(code) == 4: stocks.append({"code": f"{code}.TWO", "region": "TW"})
    return stocks

def get_us_stock_list():
    """取得 S&P 500 成分股"""
    try:
        print("正在抓取 S&P 500 成分股清單...")
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = table[0]
        symbols = df['Symbol'].values.tolist()
        formatted_symbols = []
        for s in symbols:
            formatted_symbols.append({"code": s.replace('.', '-'), "region": "US"})
        return formatted_symbols
    except Exception as e:
        print(f"抓取美股清單失敗: {e}")
        return []

def get_fundamentals(stock_obj):
    try:
        info = stock_obj.info
        pe = info.get('trailingPE')
        if pe is None: pe = 999 
        growth = info.get('earningsGrowth')
        roe = info.get('returnOnEquity')
        return {"pe": pe, "growth": growth, "roe": roe}
    except:
        return {"pe": 999, "growth": None, "roe": None}

def analyze_stock(stock_info, check_exit_stocks=None):
    ticker = stock_info['code']
    region = stock_info['region']
    
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
                    "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"觸發停損 (虧損 {loss_pct*100:.1f}%)"
                }

            # 趨勢出場
            is_ma_flattening = curr_ma65 <= prev_ma65
            if current_price < curr_ma65 and is_ma_flattening:
                return {
                    "code": ticker,
                    "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": "跌破13週線 且 趨勢轉弱"
                }
            return None

        # --- [模式 A] 掃描進場 ---
        if check_exit_stocks is not None: return None
        
        min_vol = 500000 if region == 'TW' else 1000000
        if latest['Volume'] < min_vol: return None 

        window_high = df['Close'][-LOOKBACK_LONG-1:-1].max()
        is_new_high = latest['Close'] > window_high
        
        was_high_yesterday = prev['Close'] > window_high
        is_fresh_breakout = is_new_high and (not was_high_yesterday)

        open_price = latest['Open']
        is_big_candle = (latest['Close'] - open_price) / open_price > 0.015

        if is_fresh_breakout and is_big_candle:
            
            fund_data = get_fundamentals(stock)
            
            score = 0
            reasons = ["突破2年新高", "實體紅K"]
            
            if fund_data['growth'] is not None and fund_data['growth'] > 0.15:
                score += 1
                reasons.append(f"獲利成長猛 ({fund_data['growth']*100:.0f}%)")
            elif fund_data['growth'] is not None and fund_data['growth'] > 0:
                reasons.append("獲利正成長")

            pe_val = fund_data['pe']
            if pe_val < 30: 
                score += 1
                reasons.append(f"本益比低 ({pe_val:.1f})")
            elif pe_val > MAX_PE:
                reasons.append(f"⚠️本益比過高 ({pe_val:.1f})")

            if fund_data['roe'] is not None and fund_data['roe'] > MIN_ROE:
                score += 1
                reasons.append(f"ROE優 ({fund_data['roe']*100:.1f}%)")

            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * 1.5:
                score += 1
                reasons.append("成交量爆發")

            return {
                "code": ticker,
                "region": region,
                "name": ticker, 
                "price": float(f"{latest['Close']:.2f}"),
                "score": score,
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
    print("啟動動能策略 (台美股) 掃描...")
    
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
    
    tw_stocks = get_tw_stock_list()
    us_stocks = get_us_stock_list()
    
    all_stocks = tw_stocks + us_stocks 
    
    today_buys = []
    today_exits = []

    print(f"正在掃描買點 (共 {len(all_stocks)} 檔)...")
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(analyze_stock, stock_info, None) for stock_info in all_stocks]
        for future in futures:
            res = future.result()
            if res: today_buys.append(res)

    print(f"正在檢查持倉出場 ({len(holdings_map)} 檔)...")
    if holdings_map:
        check_list = []
        for code in holdings_map.keys():
            region = 'TW' if '.TW' in code or '.TWO' in code else 'US'
            check_list.append({"code": code, "region": region})

        with ThreadPoolExecutor(max_workers=25) as executor:
            futures = [executor.submit(analyze_stock, stock, holdings_map) for stock in check_list]
            for future in futures:
                res = future.result()
                if res: today_exits.append(res)

    today_buys.sort(key=lambda x: -x['score'])
    
    # --- 重要：日期校正 ---
    # 因為我們是隔天早上 06:00 執行，所以資料是屬於 "前一天" 的盤後資訊
    # 這樣顯示在網頁上才是正確的 "Market Date"
    market_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    new_record = {
        "date": market_date,
        "buy": today_buys,
        "sell": today_exits
    }
    
    if history_data and history_data[-1]['date'] == market_date:
        history_data[-1] = new_record
    else:
        history_data.append(new_record)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=2)

    print(f"掃描完成！資料已更新至 {DATA_FILE} (Date: {market_date})")

if __name__ == "__main__":
    main()
