import yfinance as yf
import pandas as pd
import twstock
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# --- 全域設定 ---
DATA_FILE = "data.json"
tw_stock_map = twstock.codes 

# --- 模擬資料：主動式 ETF 持股庫 ---
MOCK_ETF_DB = {
    "00980A": {"name": "野村台灣創新", "holdings": {"2330.TW": {"shares": 500, "pct": 15.2}, "2317.TW": {"shares": 300, "pct": 8.5}, "2454.TW": {"shares": 100, "pct": 5.1}}},
    "00981A": {"name": "凱基優選", "holdings": {"2330.TW": {"shares": 800, "pct": 18.1}, "2303.TW": {"shares": 1200, "pct": 6.2}, "2603.TW": {"shares": 500, "pct": 4.3}}},
    "00982A": {"name": "富邦成長", "holdings": {"2330.TW": {"shares": 600, "pct": 12.0}, "2317.TW": {"shares": 400, "pct": 7.8}, "3008.TW": {"shares": 50, "pct": 3.2}}},
}

def get_stock_name(ticker, region, stock_obj=None):
    display_name = ticker
    if region == 'TW':
        clean_code = ticker.split('.')[0]
        if clean_code in tw_stock_map:
            return tw_stock_map[clean_code].name
    if stock_obj:
        try:
            return stock_obj.info.get('longName') or stock_obj.info.get('shortName') or ticker
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

# ==========================================
# 策略 1: 動能爆發 (Momentum)
# ==========================================
def strategy_momentum(df, ticker, region, latest, prev, fin_data):
    LOOKBACK_SHORT = 60
    LOOKBACK_LONG = 500
    VOL_FACTOR = 1.2
    GROWTH_REV_PRIORITY = 0.15

    min_vol = 500000 if region == 'TW' else 1000000
    if latest['Volume'] < min_vol: return None

    window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
    is_new_high = latest['Close'] > window_high_short
    was_high_yesterday = prev['Close'] > window_high_short
    
    if is_new_high and not was_high_yesterday:
        score = 3
        reasons = ["(基礎) 創季新高 +3分"]
        
        vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
        if latest['Volume'] > vol_ma20 * VOL_FACTOR:
            reasons.append(f"(基礎) 量增{VOL_FACTOR}倍")

        window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
        if latest['Close'] > window_high_long:
            score += 2
            reasons.append("(加分) 兩年新高 +2分")

        if fin_data['rev_yoy'] and fin_data['rev_yoy'] > GROWTH_REV_PRIORITY:
            score += 3
            reasons.append("★營收年增>15% (+3分)")
        elif fin_data['rev_yoy'] and fin_data['rev_yoy'] > 0:
            score += 1
            reasons.append("(加分) 營收正成長 (+1分)")
            
        return {"score": score, "reasons": reasons}
    return None

# ==========================================
# 策略 2: 葛蘭碧八大法則 (Granville MA200)
# ==========================================
def strategy_granville(df, ticker, region, latest, prev):
    if len(df) < 205: return None
    ma200 = df['Close'].rolling(window=200).mean()
    curr_ma = ma200.iloc[-1]
    prev_ma = ma200.iloc[-2]
    ma_rising = curr_ma > prev_ma
    ma_falling = curr_ma < prev_ma
    close = latest['Close']
    prev_close = prev['Close']
    
    if prev_close >= prev_ma and close < curr_ma and ma_rising:
        return {"type": "buy", "score": 5, "title": "葛蘭碧法則2 (買進)", "desc": "假跌破：跌破上揚年線，視為洗盤。", "ma200": float(f"{curr_ma:.2f}")}

    dist_to_ma = (latest['Low'] - curr_ma) / curr_ma
    if 0 < dist_to_ma < 0.015 and close > latest['Open'] and ma_rising:
        return {"type": "buy", "score": 4, "title": "葛蘭碧法則3 (買進)", "desc": "回測支撐：回測年線不破且收紅K。", "ma200": float(f"{curr_ma:.2f}")}

    if prev_close <= prev_ma and close > curr_ma and ma_falling:
        return {"type": "sell", "score": -5, "title": "葛蘭碧法則6 (賣出)", "desc": "假突破：突破下彎年線，屬逃命波。", "ma200": float(f"{curr_ma:.2f}")}

    dist_to_ma_high = (curr_ma - latest['High']) / curr_ma
    if 0 < dist_to_ma_high < 0.015 and close < latest['Open'] and ma_falling:
        return {"type": "sell", "score": -4, "title": "葛蘭碧法則7 (賣出)", "desc": "反彈遇壓：反彈至年線不過且收黑K。", "ma200": float(f"{curr_ma:.2f}")}
        
    return None

# ==========================================
# 策略 3: 隔日沖 - 強勢回檔 (Day Trading)
# ==========================================
def strategy_day_trading(df, ticker, region, latest):
    # 至少需要約 50 天資料計算 MA46
    if len(df) < 50: return None
    
    # 1. K棒條件: 長黑棒 > 3.5% (開盤 > 收盤, 實體跌幅 > 3.5%)
    open_p = latest['Open']
    close_p = latest['Close']
    if close_p >= open_p: return None # 必須是黑K (收盤 < 開盤)
    
    body_pct = (open_p - close_p) / open_p
    if body_pct <= 0.035: return None # 實體跌幅需 > 3.5%
    
    # 2. 成交量與金額條件
    # 當日成交量 > 300張 (300,000股)
    vol = latest['Volume']
    if vol < 300000: return None
    
    # 日成交金額 > 0.05億 (5000萬)
    amount = close_p * vol
    if amount < 50000000: return None
    
    # 3. 漲跌趨勢條件
    # 近 20 日漲幅 > 20%
    # 往前推 20 天的收盤價
    price_20_ago = df['Close'].iloc[-21]
    rise_20d = (close_p - price_20_ago) / price_20_ago
    if rise_20d <= 0.20: return None
    
    # 近 3 天區間漲幅 > 10% (代表急漲)
    price_3_ago = df['Close'].iloc[-4]
    rise_3d = (close_p - price_3_ago) / price_3_ago
    if rise_3d <= 0.10: return None
    
    # 4. 均線排列條件 (多頭排列)
    # 條件圖示: MA45 > MA46 (中長期多頭)
    ma45 = df['Close'].rolling(window=45).mean().iloc[-1]
    ma46 = df['Close'].rolling(window=46).mean().iloc[-1]
    
    if ma45 <= ma46: return None
    
    # 符合所有條件
    return {
        "drop_pct": round(body_pct * 100, 2),
        "rise_20d": round(rise_20d * 100, 2),
        "vol_lots": int(vol / 1000), # 張數
        "amount_yi": round(amount / 100000000, 2) # 億
    }

# ==========================================
# 策略 4: 主動式 ETF (Active ETF)
# ==========================================
def strategy_active_etf(ticker, latest_price):
    held_by = []
    total_shares = 0
    total_value = 0
    for etf_code, data in MOCK_ETF_DB.items():
        if ticker in data['holdings']:
            h = data['holdings'][ticker]
            val = h['shares'] * 1000 * latest_price
            held_by.append({"etf_code": etf_code, "etf_name": data['name'], "shares": h['shares'], "pct": h['pct'], "value": val})
            total_shares += h['shares']
            total_value += val
            
    if len(held_by) > 0:
        return {"count": len(held_by), "total_shares": total_shares, "total_value": total_value, "details": held_by}
    return None

# --- 工具 ---
def get_financial_details(stock_obj):
    data = {"pe": 999, "growth": None, "rev_yoy": None, "rev_qoq": None, "quarters": []}
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        data['growth'] = info.get('earningsGrowth', None)
        data['rev_yoy'] = info.get('revenueGrowth', None)
        q_stmt = stock_obj.quarterly_income_stmt
        if q_stmt is not None and not q_stmt.empty:
            vals = q_stmt.loc['Total Revenue'] if 'Total Revenue' in q_stmt.index else q_stmt.loc['Operating Revenue']
            limit = min(4, len(vals))
            for i in range(limit):
                curr = vals[i]
                qoq = (curr - vals[i+1]) / vals[i+1] if i+1 < len(vals) and vals[i+1] != 0 else None
                data['quarters'].append({"date": vals.index[i].strftime('%Y-%m'), "revenue": curr, "qoq": qoq})
    except: pass
    return data

def analyze_stock(stock_info):
    ticker = stock_info['code']
    region = stock_info['region']
    try:
        stock = yf.Ticker(ticker)
        # 恢復還原權值 (預設 auto_adjust=True) 以符合長期策略
        # 但隔日沖策略通常看 K 線圖 (不還原)，這裡混用時要注意
        # 為了滿足「隔日沖」通常看實際 K 線，這裡用 auto_adjust=False 比較準確抓出「長黑」
        # 但使用者上一輪說要「還原權值」。
        # 折衷：既然上一輪使用者指定「還原權值」，我們維持預設 (auto_adjust=True)。
        # 若長黑K在還原圖上成立，通常在原始圖上也成立。
        df = stock.history(period="3y") 
        if len(df) < 205: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        fin_data = get_financial_details(stock)
        display_name = get_stock_name(ticker, region, stock)
        
        base = {
            "code": ticker, "name": display_name, "region": region,
            "price": float(f"{latest['Close']:.2f}"),
            "date": latest.name.strftime('%Y-%m-%d'),
            "fundamentals": fin_data
        }
        
        pkg = {}
        has_res = False

        # 策略 1
        m_res = strategy_momentum(df, ticker, region, latest, prev, fin_data)
        if m_res: pkg['momentum'] = {**base, **m_res}; has_res = True
            
        # 策略 2
        g_res = strategy_granville(df, ticker, region, latest, prev)
        if g_res: pkg['granville'] = {**base, **g_res}; has_res = True
            
        # 策略 3 (新增)
        d_res = strategy_day_trading(df, ticker, region, latest)
        if d_res: pkg['day_trading'] = {**base, **d_res}; has_res = True

        # 策略 4
        e_res = strategy_active_etf(ticker, latest['Close'])
        if e_res: pkg['active_etf'] = {**base, **e_res}; has_res = True
            
        return pkg if has_res else None
    except: return None

def main():
    print("啟動全策略掃描 (含隔日沖)...")
    stocks = get_tw_stock_list() # + get_us_stock_list()
    res = {"momentum": [], "granville_buy": [], "granville_sell": [], "day_trading": [], "active_etf": []}
    
    with ThreadPoolExecutor(max_workers=20) as exc:
        futures = [exc.submit(analyze_stock, s) for s in stocks]
        for f in as_completed(futures):
            r = f.result()
            if r:
                if 'momentum' in r: res['momentum'].append(r['momentum'])
                if 'granville' in r:
                    if r['granville']['type'] == 'buy': res['granville_buy'].append(r['granville'])
                    else: res['granville_sell'].append(r['granville'])
                if 'day_trading' in r: res['day_trading'].append(r['day_trading'])
                if 'active_etf' in r: res['active_etf'].append(r['active_etf'])

    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d']) # 隔日沖按20日漲幅排序 (找最妖的)
    
    final = []
    market_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if os.path.exists(DATA_FILE):
        try: final = json.load(open(DATA_FILE))
        except: pass
        
    rec = {"date": market_date, "strategies": res}
    if final and final[-1]['date'] == market_date: final[-1] = rec
    else: final.append(rec)
        
    with open(DATA_FILE, 'w', encoding='utf-8') as f: json.dump(final, f, ensure_ascii=False, indent=2)
    print("掃描完成。")

if __name__ == "__main__":
    main()
