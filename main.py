import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# --- 策略參數 ---
LOOKBACK_LONG = 500    # 2年新高
MA_W13 = 65            # 13週線
STOP_LOSS_PCT = 0.08   # 停損 8%
MIN_ROE = 0.08         # ROE 8%
MAX_PE = 60            # 本益比

DATA_FILE = "data.json"

def get_tw_stock_list():
    twse = twstock.twse
    tpex = twstock.tpex
    stocks = []
    for code in twse:
        if len(code) == 4: stocks.append({"code": f"{code}.TW", "region": "TW"})
    for code in tpex:
        if len(code) == 4: stocks.append({"code": f"{code}.TWO", "region": "TW"})
    return stocks

def get_us_stock_list():
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

def get_financial_details(stock_obj):
    """
    抓取基本面 + 近四季營收 (僅針對通過技術面的股票執行)
    """
    data = {
        "pe": 999, "growth": None, "roe": None,
        "quarters": [] # 存放近四季資料
    }
    
    try:
        # 1. 基本資訊
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        if data['pe'] is None: data['pe'] = 999
        
        data['growth'] = info.get('earningsGrowth', None) # 獲利成長率
        data['roe'] = info.get('returnOnEquity', None)

        # 2. 抓取季營收 (Income Statement)
        # yfinance 的 quarterly_income_stmt 通常包含最近 4-5 個季度
        q_stmt = stock_obj.quarterly_income_stmt
        
        if q_stmt is not None and not q_stmt.empty:
            # 找到 "Total Revenue" 或 "Operating Revenue"
            # 不同的會計準則名稱可能不同，嘗試抓取
            rev_row = None
            if 'Total Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Total Revenue']
            elif 'Operating Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Operating Revenue']
                
            if rev_row is not None:
                # 取最近 4 個季度 (由新到舊)
                recent_quarters = rev_row.head(4)
                
                for date, revenue in recent_quarters.items():
                    # 嘗試簡單計算 YoY (這裡因為 yfinance 免費版限制，有時抓不到去年同期)
                    # 我們這裡先存原始數值，前端負責顯示
                    data['quarters'].append({
                        "date": date.strftime('%Y-%m'), # 顯示 2024-09 這樣
                        "revenue": revenue
                    })

    except Exception as e:
        print(f"財報抓取部分失敗: {e}")
    
    return data

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
            
            # 通過技術面後，才去抓詳細財報
            fin_data = get_financial_details(stock)
            
            score = 0
            reasons = ["突破2年新高", "實體紅K"]
            
            # 評分邏輯 (顯示具體數字在前端處理)
            if fin_data['growth'] is not None and fin_data['growth'] > 0.15:
                score += 1
                reasons.append("獲利高成長")
            elif fin_data['growth'] is not None and fin_data['growth'] > 0:
                reasons.append("獲利正成長")

            pe_val = fin_data['pe']
            if pe_val < 30: 
                score += 1
                reasons.append("本益比合理")
            elif pe_val > MAX_PE:
                reasons.append("本益比過高")

            if fin_data['roe'] is not None and fin_data['roe'] > MIN_ROE:
                score += 1
                reasons.append("ROE優秀")

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
                    "growth": "N/A" if fin_data['growth'] is None else f"{fin_data['growth']*100:.1f}%",
                    "roe": "N/A" if fin_data['roe'] is None else f"{fin_data['roe']*100:.1f}%",
                    "quarters": fin_data['quarters'] # 放入季營收資料
                },
                "date": latest.name.strftime('%Y-%m-%d')
            }
        return None

    except Exception:
        return None

def main():
    print("啟動動能策略 (含財報深挖) 掃描...")
    
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
    
    # 日期校正 (因為隔天早上跑)
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
