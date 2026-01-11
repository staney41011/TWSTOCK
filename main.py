import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- 策略參數 ---
LOOKBACK_SHORT = 60     # 基礎: 近一季新高
LOOKBACK_LONG = 500     # 加分: 兩年新高
VOL_FACTOR = 1.2        # 基礎: 成交量 > 20日均量 1.2倍
GROWTH_REV = 0.10       # 加分: 營收成長 > 10%
GROWTH_EPS = 0.20       # 加分: 獲利成長 > 20%
SELL_RATIO_THRESHOLD = 1.16 # 出場: 賣壓比例 > 116%

DATA_FILE = "data.json"

tw_stock_map = twstock.codes 

def get_stock_name(ticker, region, stock_obj=None):
    display_name = ticker
    if region == 'TW':
        clean_code = ticker.split('.')[0]
        if clean_code in tw_stock_map:
            return tw_stock_map[clean_code].name
    if stock_obj:
        try:
            long_name = stock_obj.info.get('longName')
            short_name = stock_obj.info.get('shortName')
            if long_name: return long_name
            if short_name: return short_name
        except:
            pass
    return display_name

def get_tw_stock_list():
    stocks = []
    for code in twstock.twse:
        if len(code) == 4: stocks.append({"code": f"{code}.TW", "region": "TW"})
    for code in twstock.tpex:
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
    """抓取營收與獲利成長"""
    data = {
        "pe": 999, "growth": None, "rev_yoy": None, 
        "rev_last_year": None, "quarters": []
    }
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        data['growth'] = info.get('earningsGrowth', None)
        data['rev_yoy'] = info.get('revenueGrowth', None)

        q_stmt = stock_obj.quarterly_income_stmt
        if q_stmt is not None and not q_stmt.empty:
            rev_row = None
            if 'Total Revenue' in q_stmt.index: rev_row = q_stmt.loc['Total Revenue']
            elif 'Operating Revenue' in q_stmt.index: rev_row = q_stmt.loc['Operating Revenue']
            
            if rev_row is not None:
                recent_quarters = rev_row.head(4)
                if data['rev_yoy'] is not None and len(recent_quarters) > 0:
                    try: data['rev_last_year'] = recent_quarters.iloc[0] / (1 + data['rev_yoy'])
                    except: pass
                for date, revenue in recent_quarters.items():
                    data['quarters'].append({"date": date.strftime('%Y-%m'), "revenue": revenue})
    except:
        pass
    return data

def calculate_sell_pressure(df):
    """計算賣壓比例"""
    if len(df) < 22: return 0
    subset = df.iloc[-21:] 
    total_buy_vol = 0
    total_sell_vol = 0
    
    for i in range(1, len(subset)):
        today = subset.iloc[i]
        prev = subset.iloc[i-1]
        high = today['High']; low = today['Low']; close = today['Close']
        prev_close = prev['Close']; vol = today['Volume']
        
        up_power = max(0, high - prev_close) + max(0, close - low)
        down_power = high - low
        total_power = up_power + down_power
        
        if total_power == 0:
            buy_part = vol * 0.5; sell_part = vol * 0.5
        else:
            buy_part = (up_power / total_power) * vol
            sell_part = (down_power / total_power) * vol
            
        total_buy_vol += buy_part
        total_sell_vol += sell_part
        
    if total_buy_vol == 0: return 999
    return total_sell_vol / total_buy_vol

def analyze_stock(stock_info, check_exit_stocks=None):
    ticker = stock_info['code']
    region = stock_info['region']
    
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="3y") 
        if len(df) < LOOKBACK_LONG + 10: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        display_name = get_stock_name(ticker, region, stock)

        # --- [出場檢查] ---
        if check_exit_stocks and ticker in check_exit_stocks:
            buy_price = check_exit_stocks[ticker]
            current_price = latest['Close']
            
            sell_ratio = calculate_sell_pressure(df)
            
            if sell_ratio > SELL_RATIO_THRESHOLD:
                return {
                    "type": "sell",
                    "code": ticker, "name": display_name, "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"賣壓比例過高 ({sell_ratio*100:.1f}%)"
                }

            loss_pct = (current_price - buy_price) / buy_price
            if loss_pct <= -0.08:
                return {
                    "type": "sell",
                    "code": ticker, "name": display_name, "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"觸發停損 (虧損 {loss_pct*100:.1f}%)"
                }
            return None

        # --- [進場檢查] ---
        if check_exit_stocks is not None: return None
        
        min_vol = 500000 if region == 'TW' else 1000000
        if latest['Volume'] < min_vol: return None 

        # 基礎: 季新高 + 首度突破
        window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
        is_new_high_short = latest['Close'] > window_high_short
        was_high_yesterday = prev['Close'] > window_high_short
        is_fresh_breakout = is_new_high_short and (not was_high_yesterday)

        if is_fresh_breakout:
            score = 3
            # 將基礎分列入明細，方便 Modal 顯示
            reasons = ["(基礎) 創季新高 +3分"]
            
            # 基礎: 量能檢查
            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * VOL_FACTOR:
                reasons.append(f"(基礎) 量增{VOL_FACTOR}倍")
            else:
                # 若無量，雖創新高但扣分或不列入理由(視策略而定，這裡保留但沒加分)
                pass

            # 加分項 1: 兩年新高
            window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
            if latest['Close'] > window_high_long:
                score += 2
                reasons.append("(加分) 兩年新高 +2分")

            # 加分項 2: 基本面
            fin_data = get_financial_details(stock)
            
            if fin_data['rev_yoy'] is not None and fin_data['rev_yoy'] > GROWTH_REV:
                score += 2
                reasons.append(f"(加分) 營收增{fin_data['rev_yoy']*100:.0f}% +2分")
            
            if fin_data['growth'] is not None and fin_data['growth'] > GROWTH_EPS:
                score += 2
                reasons.append(f"(加分) 獲利增{fin_data['growth']*100:.0f}% +2分")
            
            # 加分項 3: 風險控制 (原振幅濾網改為加分項? 或者直接拿掉。使用者說拿掉篩選，這裡就不扣分也不擋)
            # 這裡可以改成: 若振幅適中給分，若漲停鎖死(振幅小)也給分，故不特別處理
            
            return {
                "type": "buy",
                "code": ticker, "name": display_name, "region": region,
                "price": float(f"{latest['Close']:.2f}"),
                "score": score,
                "reasons": reasons,
                "fundamentals": {
                    "pe": "N/A" if fin_data['pe']==999 else f"{fin_data['pe']:.1f}",
                    "growth": "N/A" if fin_data['growth'] is None else f"{fin_data['growth']*100:.1f}%",
                    "rev_yoy": fin_data['rev_yoy'],
                    "rev_last_year": fin_data['rev_last_year'],
                    "quarters": fin_data['quarters']
                },
                "date": latest.name.strftime('%Y-%m-%d')
            }
            
        if is_new_high_short: return {"type": "stat_only", "is_new_high": True}
        return {"type": "stat_only", "is_new_high": False}

    except:
        return None

def main():
    print("啟動動能爆發策略 (無振幅濾網版)...")
    
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
    stat_total_scanned = 0
    stat_new_high_count = 0

    print(f"正在掃描 ({len(all_stocks)} 檔)...")
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(analyze_stock, stock_info, None) for stock_info in all_stocks]
        for future in as_completed(futures):
            res = future.result()
            if res:
                stat_total_scanned += 1
                if res.get('type') == 'buy':
                    today_buys.append(res)
                    stat_new_high_count += 1
                elif res.get('type') == 'stat_only' and res['is_new_high']:
                    stat_new_high_count += 1

    print(f"檢查出場 ({len(holdings_map)} 檔)...")
    if holdings_map:
        check_list = []
        for code in holdings_map.keys():
            region = 'TW' if '.TW' in code or '.TWO' in code else 'US'
            check_list.append({"code": code, "region": region})

        with ThreadPoolExecutor(max_workers=25) as executor:
            futures = [executor.submit(analyze_stock, stock, holdings_map) for stock in check_list]
            for future in futures:
                res = future.result()
                if res and res['type'] == 'sell': 
                    today_exits.append(res)

    today_buys.sort(key=lambda x: -x['score'])
    
    market_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    market_breadth_pct = 0
    if stat_total_scanned > 0:
        market_breadth_pct = round((stat_new_high_count / stat_total_scanned) * 100, 2)
    
    new_record = {
        "date": market_date,
        "market_breadth": market_breadth_pct,
        "buy": today_buys,
        "sell": today_exits
    }
    
    if history_data and history_data[-1]['date'] == market_date:
        history_data[-1] = new_record
    else:
        history_data.append(new_record)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=2)

    print(f"掃描完成！新高佔比: {market_breadth_pct}%")

if __name__ == "__main__":
    main()
