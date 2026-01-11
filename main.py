import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# --- 策略參數 ---
LOOKBACK_SHORT = 60    # [修正] 基本門檻：近一季新高 (約60交易日)
LOOKBACK_LONG = 500    # [加分] 超級門檻：近兩年新高
MA_W13 = 65            # 13週線 (趨勢判斷)
STOP_LOSS_PCT = 0.08   # 停損 8%
MIN_ROE = 0.08         # ROE 8%
MAX_PE = 60            # 本益比

DATA_FILE = "data.json"

# 建立台股代號轉中文名稱的字典
tw_codes = twstock.codes
def get_tw_name(code_full):
    try:
        # code_full 格式為 "2330.TW", 我們只要 "2330"
        code = code_full.split('.')[0]
        if code in tw_codes:
            return tw_codes[code].name
    except:
        pass
    return code_full # 抓不到就回傳代號

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
    """抓取基本面 + 近四季營收"""
    data = {
        "pe": 999, "growth": None, "roe": None,
        "name_us": None, # 美股名稱
        "quarters": []
    }
    
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        if data['pe'] is None: data['pe'] = 999
        
        data['growth'] = info.get('earningsGrowth', None)
        data['roe'] = info.get('returnOnEquity', None)
        data['name_us'] = info.get('shortName', None) # 抓取美股名稱

        # 抓取季營收
        q_stmt = stock_obj.quarterly_income_stmt
        if q_stmt is not None and not q_stmt.empty:
            rev_row = None
            if 'Total Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Total Revenue']
            elif 'Operating Revenue' in q_stmt.index:
                rev_row = q_stmt.loc['Operating Revenue']
                
            if rev_row is not None:
                recent_quarters = rev_row.head(4) # 取最近4季
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
        # 抓取 3 年資料以確保長短週期都能計算
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
            
            # 中文名稱補全 (如果之前存的是代號)
            display_name = ticker
            if region == 'TW': display_name = get_tw_name(ticker)

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

        # 1. 策略基礎：近一季新高 (60日)
        window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
        is_new_high_short = latest['Close'] > window_high_short
        
        # 昨天不能創新高 (確保是第一根突破)
        was_high_yesterday = prev['Close'] > window_high_short
        is_fresh_breakout = is_new_high_short and (not was_high_yesterday)

        # 實體紅K check
        open_price = latest['Open']
        is_big_candle = (latest['Close'] - open_price) / open_price > 0.015

        if is_fresh_breakout and is_big_candle:
            
            # 通過初步篩選，抓基本面
            fin_data = get_financial_details(stock)
            
            # 處理名稱
            stock_name = ticker
            if region == 'TW':
                stock_name = get_tw_name(ticker)
            elif fin_data['name_us']:
                stock_name = fin_data['name_us']

            score = 3 # 基礎分
            reasons = ["突破季線新高", "實體紅K"]
            
            # --- 加分項目 ---
            
            # 1. 兩年新高 (超級強勢股)
            window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
            if latest['Close'] > window_high_long:
                score += 2
                reasons.append("★突破兩年新高")

            # 2. 基本面加分
            if fin_data['growth'] is not None and fin_data['growth'] > 0.15:
                score += 1
                reasons.append("獲利高成長")
            
            pe_val = fin_data['pe']
            if pe_val < 30: 
                score += 1
            elif pe_val > MAX_PE:
                reasons.append("本益比過高") # 扣分項或警示，這裡不減分但標註

            if fin_data['roe'] is not None and fin_data['roe'] > MIN_ROE:
                score += 1
                reasons.append("ROE優秀")

            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * 1.5:
                score += 1
                reasons.append("成交量爆發")

            return {
                "code": ticker,
                "name": stock_name, # 這裡現在是中文名或美股全名
                "region": region,
                "price": float(f"{latest['Close']:.2f}"),
                "score": score,
                "reasons": reasons,
                "fundamentals": {
                    "pe": "N/A" if pe_val==999 else f"{pe_val:.1f}",
                    "growth": "N/A" if fin_data['growth'] is None else f"{fin_data['growth']*100:.1f}%",
                    "roe": "N/A" if fin_data['roe'] is None else f"{fin_data['roe']*100:.1f}%",
                    "quarters": fin_data['quarters']
                },
                "date": latest.name.strftime('%Y-%m-%d')
            }
        return None

    except Exception:
        return None

def main():
    print("啟動動能策略 (一季新高 + 兩年加分 + 中文名) 掃描...")
    
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
    
    # 日期校正
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
