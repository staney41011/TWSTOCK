import yfinance as yf
import pandas as pd
import twstock
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- 策略參數 (林則行設定) ---
LOOKBACK_SHORT = 60     # 訊號1: 近一季新高
LOOKBACK_LONG = 500     # 訊號5: 兩年新高 (輔助判斷)
VOL_FACTOR = 1.2        # 訊號4: 成交量 > 20日均量 1.2倍
AMP_LOW_RISK = 0.04     # 訊號4: 振幅 < 4% (低風險)
AMP_MED_RISK = 0.06     # 訊號4: 振幅 4%~6% (中風險)
GROWTH_REV = 0.10       # 訊號3: 營收成長 > 10%
GROWTH_EPS = 0.20       # 訊號3: 獲利成長 > 20%
SELL_RATIO_THRESHOLD = 1.16 # 賣壓比例 > 116% 出場

DATA_FILE = "data.json"

# 預先載入台股代號表
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
    """抓取訊號2 & 3: 營收與獲利成長"""
    data = {
        "pe": 999, "growth": None, "rev_yoy": None, 
        "rev_last_year": None, "quarters": []
    }
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        data['growth'] = info.get('earningsGrowth', None) # EPS成長 (對應訊號3: >20%)
        data['rev_yoy'] = info.get('revenueGrowth', None) # 營收成長 (對應訊號3: >10%)

        # 抓取季營收細節
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
    """
    計算林則行獨創「賣壓比例」
    統計過去 20 日的買盤與賣盤量
    """
    if len(df) < 22: return 0 # 資料不足
    
    # 只取最後 20 天 (要包含前一日以計算 PrevClose)
    # 實際上我們需要 iterrows 來跑每一天的邏輯
    subset = df.iloc[-21:] 
    
    total_buy_vol = 0
    total_sell_vol = 0
    
    # 從第 2 筆資料開始 (因為第 1 筆是拿來當 PrevClose 的)
    for i in range(1, len(subset)):
        today = subset.iloc[i]
        prev = subset.iloc[i-1]
        
        high = today['High']
        low = today['Low']
        close = today['Close']
        prev_close = prev['Close']
        vol = today['Volume']
        
        # 林則行公式：
        # 上漲波 (Up Power) = (High - PrevClose [若>0]) + (Close - Low [若>0])
        up_power = max(0, high - prev_close) + max(0, close - low)
        
        # 下跌波 (Down Power) = High - Low (當日全振幅視為潛在賣壓區間?) 
        # 依書中範例: 930(High)跌至905(Low)共下跌25 -> Down = High - Low
        down_power = high - low
        
        total_power = up_power + down_power
        
        if total_power == 0:
            buy_part = vol * 0.5
            sell_part = vol * 0.5
        else:
            buy_part = (up_power / total_power) * vol
            sell_part = (down_power / total_power) * vol
            
        total_buy_vol += buy_part
        total_sell_vol += sell_part
        
    if total_buy_vol == 0: return 999 # 避免除以零
    
    # 賣壓比例 = 賣出總量 / 買進總量
    ratio = total_sell_vol / total_buy_vol
    return ratio

def analyze_stock(stock_info, check_exit_stocks=None):
    ticker = stock_info['code']
    region = stock_info['region']
    
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="3y") 
        if len(df) < LOOKBACK_LONG + 10: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 取得名稱
        display_name = get_stock_name(ticker, region, stock)

        # --- [模式 B] 掃描出場 (賣壓比例) ---
        if check_exit_stocks and ticker in check_exit_stocks:
            buy_price = check_exit_stocks[ticker]
            current_price = latest['Close']
            
            # 計算賣壓比例
            sell_ratio = calculate_sell_pressure(df)
            
            # 1. 賣壓比例出場
            if sell_ratio > SELL_RATIO_THRESHOLD:
                return {
                    "type": "sell",
                    "code": ticker,
                    "name": display_name,
                    "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"賣壓比例過高 ({sell_ratio*100:.1f}%)"
                }

            # 2. 停損 (原本策略保留)
            loss_pct = (current_price - buy_price) / buy_price
            if loss_pct <= -0.08: # 8% 停損
                return {
                    "type": "sell",
                    "code": ticker,
                    "name": display_name,
                    "region": region,
                    "price": float(f"{current_price:.2f}"),
                    "date": latest.name.strftime('%Y-%m-%d'),
                    "reason": f"觸發停損 (虧損 {loss_pct*100:.1f}%)"
                }
            return None

        # --- [模式 A] 掃描進場 ---
        if check_exit_stocks is not None: return None
        
        # 成交量濾網 (訊號4: 有量)
        min_vol = 500000 if region == 'TW' else 1000000
        if latest['Volume'] < min_vol: return None 

        # 訊號1: 創近一季(60日)新高
        window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
        is_new_high_short = latest['Close'] > window_high_short
        
        # 濾網: 首度突破
        was_high_yesterday = prev['Close'] > window_high_short
        is_fresh_breakout = is_new_high_short and (not was_high_yesterday)

        if is_fresh_breakout:
            # 進入詳細檢查
            
            score = 3
            reasons = ["訊號1:創季新高"]
            
            # 訊號4: 價量同步 (成交量 > 20日均量 1.2倍)
            vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            if latest['Volume'] > vol_ma20 * VOL_FACTOR:
                score += 1
                reasons.append(f"訊號4:量增{VOL_FACTOR}倍")
            
            # 訊號4: 振幅檢查 (Risk Check)
            # 振幅 = (High - Low) / Open
            amplitude = (latest['High'] - latest['Low']) / latest['Open']
            if amplitude < AMP_LOW_RISK:
                score += 1
                reasons.append("訊號4:低風險(振幅<4%)")
            elif amplitude < AMP_MED_RISK:
                reasons.append("中風險(振幅4~6%)")
            else:
                reasons.append("⚠️高風險(振幅>6%)")

            # 訊號2 & 3: 基本面檢查
            fin_data = get_financial_details(stock)
            
            # 訊號3: 營收成長 > 10%
            if fin_data['rev_yoy'] is not None and fin_data['rev_yoy'] > GROWTH_REV:
                score += 2
                reasons.append(f"訊號3:營收增{fin_data['rev_yoy']*100:.0f}%")
            
            # 訊號3: 獲利成長 > 20%
            if fin_data['growth'] is not None and fin_data['growth'] > GROWTH_EPS:
                score += 2
                reasons.append(f"訊號3:獲利增{fin_data['growth']*100:.0f}%")
            
            # 訊號5: 兩年新高 (加分項)
            window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
            if latest['Close'] > window_high_long:
                score += 1
                reasons.append("訊號5:兩年新高")

            return {
                "type": "buy",
                "code": ticker,
                "name": display_name,
                "region": region,
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
            
        # 即使沒入選，如果是創新高，也要回傳一個標記供統計市場寬度
        if is_new_high_short:
             return {"type": "stat_only", "is_new_high": True}
             
        return {"type": "stat_only", "is_new_high": False}

    except:
        return None

def main():
    print("啟動林則行完全攻略掃描 (含賣壓比例)...")
    
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
    
    # 訊號5: 市場寬度統計
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
                elif res.get('type') == 'stat_only':
                    if res['is_new_high']:
                        stat_new_high_count += 1

    # 檢查出場
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
                if res and res['type'] == 'sell': 
                    today_exits.append(res)

    today_buys.sort(key=lambda x: -x['score'])
    
    market_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 計算市場寬度 %
    market_breadth_pct = 0
    if stat_total_scanned > 0:
        market_breadth_pct = round((stat_new_high_count / stat_total_scanned) * 100, 2)
    
    new_record = {
        "date": market_date,
        "market_breadth": market_breadth_pct, # 存入 JSON
        "buy": today_buys,
        "sell": today_exits
    }
    
    if history_data and history_data[-1]['date'] == market_date:
        history_data[-1] = new_record
    else:
        history_data.append(new_record)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(history_data, f, ensure_ascii=False, indent=2)

    print(f"掃描完成！新高家數佔比: {market_breadth_pct}%")

if __name__ == "__main__":
    main()
