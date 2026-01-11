import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# --- 策略參數 ---
LOOKBACK_SHORT = 60    # 近一季新高
LOOKBACK_LONG = 500    # 兩年新高 (加分)
MA_W13 = 65            # 13週線
STOP_LOSS_PCT = 0.08   # 停損 8%
MIN_ROE = 0.08         # ROE 8%
MAX_PE = 60            # 本益比

DATA_FILE = "data.json"

# --- 預先載入台股代號對照表 (加速查詢) ---
# twstock.codes 是一個字典，Key 是代號 (如 '2330')，Value 是 StockCodeInfo 物件
tw_stock_map = twstock.codes 

def get_stock_name(ticker, region):
    """
    取得股票名稱：
    - 台股：回傳中文名 (例如：台積電)
    - 美股：回傳英文簡稱
    """
    if region == 'TW':
        # 移除 .TW 或 .TWO 後綴
        clean_code = ticker.split('.')[0]
        if clean_code in tw_stock_map:
            return tw_stock_map[clean_code].name
    # 若找不到或是美股，回傳代號即可 (稍後 yfinance 會嘗試補美股名)
    return ticker

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
    except:
        return []

def get_financial_details(stock_obj):
    """抓取基本面 + 營收 YoY 資訊"""
    data = {
        "pe": 999, "growth": None, "roe": None,
        "name_us": None, 
        "rev_yoy": None,      # 營收年增率
        "rev_last_year": None,# 去年同期營收 (推算)
        "quarters": []
    }
    
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        if data['pe'] is None: data['pe'] = 999
        
        data['growth'] = info.get('earningsGrowth', None) # 獲利成長
        data['roe'] = info.get('returnOnEquity', None)
        data['name_us'] = info.get('shortName', None)
        
        # 抓取營收年增率 (Revenue Growth YoY)
        # yfinance 的 revenueGrowth 通常是指最近一季與去年同期相比
        data['rev_yoy'] = info.get('revenueGrowth', None)

        # 抓取季營收 (Income Statement)
        q_stmt = stock_obj.quarterly_income_stmt
        if q_stmt is not None and not q_stmt.empty:
            rev_row = None
            if 'Total Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Total Revenue']
            elif 'Operating Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Operating Revenue']
                
            if rev_row is not None:
                # 放入近四季資料
                recent_quarters = rev_row.head(4)
                
                # 計算去年同期營收 (如果我們有年增率和這一季營收，可以反推)
                # Last Year = Current / (1 + Growth)
                if data['rev_yoy'] is not None and len(recent_quarters) > 0:
                    current_rev = recent_quarters.iloc[0]
                    try:
                        data['rev_last_year'] = current_rev / (1 + data['rev_yoy'])
                    except:
                        data['rev_last_year'] = 0

                for date, revenue in recent_quarters.items():
                    data['quarters'].append({
                        "date": date.strftime('%Y-%m'),
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

        # 預先取得名稱
        display_name = get_stock_name(ticker, region)

        # --- [模式 B] 掃描出場 ---
        if check_exit_stocks and ticker in check_exit_stocks:
            buy_price = check_exit_stocks[ticker]
            current_price = latest['Close']
            
            # 停損
            loss_pct = (current_price - buy_price) / buy_price
            if loss_pct <= -STOP_LOSS_PCT:
                return {
                    "code": ticker,
                    "name": display_name,
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
                    "name": display_name,
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

        window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
        is_new_high_short = latest['Close'] > window_high_short
        
        was_high_yesterday = prev['Close'] > window_high_short
        is_fresh_breakout = is_new_high_short and (not was_high_yesterday)

        open_price = latest['Open']
        is_big_candle = (latest['Close'] - open_price) / open_price > 0.015

        if is_fresh_breakout and is_big_candle:
            
            fin_data = get_financial_details(stock)
            
            # 美股名稱補全
            if region == 'US' and fin_data['name_us']:
                display_name = fin_data['name_us']

            score = 3
            reasons = ["突破季線新高", "實體紅K"]
            
            # 加分項
            window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
            if latest['Close'] > window_high_long:
                score += 2
                reasons.append("★突破兩年新高")

            # 營收/獲利加分
            if fin_data['rev_yoy'] is not None and fin_data['rev_yoy'] > 0.2:
                 score += 1 # 營收成長 > 20%
                 reasons.append("營收大爆發")
            
            if fin_data['growth'] is not None and fin_data['growth'] > 0.15:
                score += 1
                reasons.append("EPS高成長")
            
            pe_val = fin_data['pe']
            if pe_val < 30: score += 1

            if fin_data['roe'] is not None and fin_data['roe'] > MIN_ROE:
                score += 1
                reasons.append("ROE優秀")

            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * 1.5:
                score += 1
                reasons.append("成交量爆發")

            return {
                "code": ticker,
                "name": display_name,
                "region": region,
                "price": float(f"{latest['Close']:.2f}"),
                "score": score,
                "reasons": reasons,
                "fundamentals": {
                    "pe": "N/A" if pe_val==999 else f"{pe_val:.1f}",
                    "growth": "N/A" if fin_data['growth'] is None else f"{fin_data['growth']*100:.1f}%",
                    "roe": "N/A" if fin_data['roe'] is None else f"{fin_data['roe']*100:.1f}%",
                    "rev_yoy": fin_data['rev_yoy'], # 新增: 營收年增率
                    "rev_last_year": fin_data['rev_last_year'], # 新增: 去年同期營收
                    "quarters": fin_data['quarters']
                },
                "date": latest.name.strftime('%Y-%m-%d')
            }
        return None

    except Exception:
        return None

def main():
    print("啟動動能策略 (Git修復 + 營收YoY版) 掃描...")
    
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
