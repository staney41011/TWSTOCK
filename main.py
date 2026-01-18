import yfinance as yf
import pandas as pd
import twstock
import json
import os
import random
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

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
    LOOKBACK_SHORT = 60; LOOKBACK_LONG = 500; VOL_FACTOR = 1.2; GROWTH_REV_PRIORITY = 0.15
    if latest['Volume'] < (500000 if region == 'TW' else 1000000): return None
    window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
    if latest['Close'] > window_high_short and prev['Close'] <= window_high_short:
        score = 3
        reasons = ["(基礎) 創季新高 +3分"]
        vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
        if latest['Volume'] > vol_ma20 * VOL_FACTOR: reasons.append(f"(基礎) 量增{VOL_FACTOR}倍")
        window_high_long = df['Close'][-LOOKBACK_LONG-1:-1].max()
        if latest['Close'] > window_high_long: score += 2; reasons.append("(加分) 兩年新高 +2分")
        if fin_data['rev_yoy'] and fin_data['rev_yoy'] > GROWTH_REV_PRIORITY: score += 3; reasons.append("★營收年增>15% (+3分)")
        elif fin_data['rev_yoy'] and fin_data['rev_yoy'] > 0: score += 1; reasons.append("(加分) 營收正成長 (+1分)")
        if fin_data['growth'] and fin_data['growth'] > 0.15: score += 1; reasons.append("(加分) EPS高成長 (+1分)")
        if fin_data['pe'] != 999 and fin_data['pe'] < 30: score += 1; reasons.append("(加分) 本益比合理 (+1分)")
        return {"score": score, "reasons": reasons}
    return None

# ==========================================
# 策略 2: 葛蘭碧八大法則 (MA200)
# ==========================================
def strategy_granville(df, ticker, region, latest, prev):
    if len(df) < 205: return None
    ma200 = df['Close'].rolling(window=200).mean(); curr_ma = ma200.iloc[-1]; prev_ma = ma200.iloc[-2]
    ma_rising = curr_ma > prev_ma; ma_falling = curr_ma < prev_ma
    close = latest['Close']; prev_close = prev['Close']
    if prev_close >= prev_ma and close < curr_ma and ma_rising: return {"type": "buy", "score": 5, "title": "葛蘭碧法則2 (買進)", "desc": "假跌破：跌破上揚年線，視為洗盤。", "ma200": float(f"{curr_ma:.2f}")}
    dist = (latest['Low'] - curr_ma) / curr_ma
    if 0 < dist < 0.015 and close > latest['Open'] and ma_rising: return {"type": "buy", "score": 4, "title": "葛蘭碧法則3 (買進)", "desc": "回測支撐：回測年線不破且收紅K。", "ma200": float(f"{curr_ma:.2f}")}
    if prev_close <= prev_ma and close > curr_ma and ma_falling: return {"type": "sell", "score": -5, "title": "葛蘭碧法則6 (賣出)", "desc": "假突破：突破下彎年線，屬逃命波。", "ma200": float(f"{curr_ma:.2f}")}
    dist_h = (curr_ma - latest['High']) / curr_ma
    if 0 < dist_h < 0.015 and close < latest['Open'] and ma_falling: return {"type": "sell", "score": -4, "title": "葛蘭碧法則7 (賣出)", "desc": "反彈遇壓：反彈至年線不過且收黑K。", "ma200": float(f"{curr_ma:.2f}")}
    return None

# ==========================================
# 策略 3: 隔日沖 - 強勢回檔
# ==========================================
def strategy_day_trading(df, ticker, region, latest):
    if len(df) < 50: return None
    ma3 = df['Close'].rolling(3).mean().iloc[-1]; ma4 = df['Close'].rolling(4).mean().iloc[-1]
    ma45 = df['Close'].rolling(45).mean().iloc[-1]; ma46 = df['Close'].rolling(46).mean().iloc[-1]
    if not (ma3 > ma4 and ma45 > ma46): return None
    today = df.iloc[-1]
    if today['Close'] >= today['Open']: return None
    day_prev = df.iloc[-2]; day_prev_2 = df.iloc[-3]
    if (day_prev['Close'] - day_prev_2['Close']) / day_prev_2['Close'] < 0.095: return None
    if day_prev_2['Close'] <= day_prev_2['Open']: return None
    price_20_ago = df['Close'].iloc[-21]
    if (today['Close'] - price_20_ago) / price_20_ago <= 0.20: return None
    if today['Volume'] < 300000: return None
    if today['Close'] * today['Volume'] < 50000000: return None
    return {"drop_pct": round(((today['Open'] - today['Close']) / today['Open']) * 100, 2), "rise_20d": round(((today['Close'] - price_20_ago) / price_20_ago) * 100, 2), "vol_lots": int(today['Volume'] / 1000), "amount_yi": round((today['Close'] * today['Volume']) / 100000000, 2), "pattern": "連紅漲停後黑K"}

# ==========================================
# 策略 4: 十字星主升起漲
# ==========================================
def strategy_doji_rise(df, ticker, region, latest):
    if len(df) < 65: return None
    close = latest['Close']; open_p = latest['Open']; vol = latest['Volume']
    ma5_vol = df['Volume'].rolling(5).mean().iloc[-1]
    ma20 = df['Close'].rolling(20).mean().iloc[-1]
    ma60 = df['Close'].rolling(60).mean().iloc[-1]; ma60_prev = df['Close'].rolling(60).mean().iloc[-2]
    if not (ma5_vol >= 5000000 or (ma5_vol * df['Close'][-5:].mean()) >= 1000000000): return None
    if close < ma20 or close < ma60 or ma60 < ma60_prev or close/ma20 > 1.15: return None
    body_pct = abs(close - open_p) / open_p
    if body_pct > 0.006: return None
    total_range = latest['High'] - latest['Low']
    if total_range < abs(close - open_p) * 2 or total_range == 0: return None
    vol_ratio = vol / ma5_vol
    if vol_ratio > 1.5 or vol_ratio < 0.5: return None
    score = 60; reasons = ["結構+十字星成立 (60分)"]
    if ma5_vol >= 10000000: score += 5; reasons.append("流動性極佳 (+5)")
    if 0.8 <= vol_ratio <= 1.2: score += 5; reasons.append("量能平穩 (+5)")
    ma5 = df['Close'].rolling(5).mean().iloc[-1]; ma10 = df['Close'].rolling(10).mean().iloc[-1]
    if ma5 > ma10 > ma20 > ma60: score += 5; reasons.append("均線多頭排列 (+5)")
    if ma5_vol < 6000000: score -= 10; reasons.append("流動性邊緣 (-10)")
    if vol_ratio > 1.3: score -= 5; reasons.append("量能稍大 (-5)")
    if score < 60: return None
    return {"score": score, "pattern": "標準十字星", "vol_ratio": round(vol_ratio * 100, 1), "vol_avg_val": round((ma5_vol * df['Close'][-5:].mean()) / 100000000, 1), "trend": "多頭整理", "reasons": reasons}

# ==========================================
# 策略 5: 主動式 ETF
# ==========================================
def strategy_active_etf(ticker, latest_price):
    held_by = []
    total_shares = 0; total_value = 0
    for etf_code, data in MOCK_ETF_DB.items():
        if ticker in data['holdings']:
            h = data['holdings'][ticker]
            val = h['shares'] * 1000 * latest_price
            held_by.append({"etf_code": etf_code, "etf_name": data['name'], "shares": h['shares'], "pct": h['pct'], "value": val})
            total_shares += h['shares']; total_value += val
    if len(held_by) > 0: return {"count": len(held_by), "total_shares": total_shares, "total_value": total_value, "details": held_by}
    return None

# ==========================================
# 策略 6: 厚積薄發 (低波動醞釀) - New!
# ==========================================
def strategy_low_volatility(df, ticker, region, latest):
    # 需要 200MA，至少要 200 根 K 棒
    if len(df) < 200: return None
    
    close = latest['Close']
    vol = latest['Volume']
    
    # 1. 計算指標
    ma50 = df['Close'].rolling(window=50).mean().iloc[-1]
    ma200 = df['Close'].rolling(window=200).mean().iloc[-1]
    vol_ma50 = df['Volume'].rolling(window=50).mean().iloc[-1]
    
    # 計算 10日標準差 (衡量波動率)
    std_dev_10 = df['Close'].rolling(window=10).std().iloc[-1]
    
    # 2. 條件 A (大趨勢): 股價 > 200MA 且 50MA > 200MA
    cond_a = close > ma200 and ma50 > ma200
    
    # 3. 條件 B (動能/支撐): 股價 > 50MA
    cond_b = close > ma50
    
    if not (cond_a and cond_b): return None # 趨勢不對直接剔除
    
    # 4. 條件 C (VCP 波動收縮)
    # 公式: 10日標準差 / 收盤價
    volatility_ratio = std_dev_10 / close
    cond_c = volatility_ratio < 0.03 # 波動極小 (3%以內)
    
    # 5. 條件 D (量能)
    # 當日量 > 50日均量 * 0.7 (不能是窒息量，至少要有一定人氣)
    cond_d = vol > vol_ma50 * 0.7
    
    # --- 判斷輸出 ---
    
    # 狀態 1: ★ META 買點 (全部符合)
    if cond_c and cond_d:
        return {
            "tag": "META",
            "volatility_pct": round(volatility_ratio * 100, 2),
            "trend_status": "多頭排列 (200MA上)",
            "volume_status": "量能達標",
            "desc": "波動壓縮極致(<3%)，且量能維持一定水準，隨時可能發動。"
        }
        
    # 狀態 2: 觀察中 (趨勢對，但波動還不夠小，或量能不足)
    # 為了不讓觀察名單太長，我們只抓波動率 < 6% 的
    elif volatility_ratio < 0.06:
        return {
            "tag": "OBSERVE",
            "volatility_pct": round(volatility_ratio * 100, 2),
            "trend_status": "多頭排列",
            "volume_status": "量能待觀察" if not cond_d else "量能達標",
            "desc": "趨勢正確，但波動尚未收斂至極致(3%~6%)，持續觀察。"
        }
        
    return None

# --- 工具 & Main ---
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
        df = stock.history(period="3y") 
        if len(df) < 205: return None
        latest = df.iloc[-1]; prev = df.iloc[-2]
        window_high_short = df['Close'][-61:-1].max()
        is_60d_high = latest['Close'] > window_high_short
        fin_data = get_financial_details(stock)
        display_name = get_stock_name(ticker, region, stock)
        base = {"code": ticker, "name": display_name, "region": region, "price": float(f"{latest['Close']:.2f}"), "date": latest.name.strftime('%Y-%m-%d'), "fundamentals": fin_data}
        pkg = {}; has_res = False
        
        if res := strategy_momentum(df, ticker, region, latest, prev, fin_data): pkg['momentum'] = {**base, **res}; has_res = True
        if res := strategy_granville(df, ticker, region, latest, prev): pkg['granville'] = {**base, **res}; has_res = True
        if res := strategy_day_trading(df, ticker, region, latest): pkg['day_trading'] = {**base, **res}; has_res = True
        if res := strategy_doji_rise(df, ticker, region, latest): pkg['doji_rise'] = {**base, **res}; has_res = True
        if res := strategy_active_etf(ticker, latest['Close']): pkg['active_etf'] = {**base, **res}; has_res = True
        # New Strategy
        if res := strategy_low_volatility(df, ticker, region, latest): pkg['low_volatility'] = {**base, **res}; has_res = True
            
        return {"result": pkg if has_res else None, "is_60d_high": is_60d_high}
    except: return None

def main():
    print("啟動全策略掃描 (含厚積薄發VCP)...")
    tw_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(tw_tz).strftime('%Y-%m-%d')
    current_hour = datetime.now(tw_tz).hour
    
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f: history = json.load(f)
            if current_hour < 14:
                original = len(history)
                history = [r for r in history if r['date'] != today_str]
                if len(history) != original: 
                    with open(DATA_FILE, 'w', encoding='utf-8') as f: json.dump(history, f, ensure_ascii=False, indent=2)
        except: pass

    stocks = get_tw_stock_list() # + get_us_stock_list()
    res = {"momentum": [], "granville_buy": [], "granville_sell": [], "day_trading": [], "doji_rise": [], "active_etf": [], "low_volatility": []}
    stat_total = 0; stat_new_high = 0
    
    with ThreadPoolExecutor(max_workers=20) as exc:
        futures = [exc.submit(analyze_stock, s) for s in stocks]
        for f in as_completed(futures):
            ret = f.result()
            if ret:
                stat_total += 1
                if ret['is_60d_high']: stat_new_high += 1
                if r := ret['result']:
                    if 'momentum' in r: res['momentum'].append(r['momentum'])
                    if 'granville' in r:
                        if r['granville']['type'] == 'buy': res['granville_buy'].append(r['granville'])
                        else: res['granville_sell'].append(r['granville'])
                    if 'day_trading' in r: res['day_trading'].append(r['day_trading'])
                    if 'doji_rise' in r: res['doji_rise'].append(r['doji_rise'])
                    if 'active_etf' in r: res['active_etf'].append(r['active_etf'])
                    if 'low_volatility' in r: res['low_volatility'].append(r['low_volatility'])

    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d'])
    res['doji_rise'].sort(key=lambda x: -x['score'])
    # 厚積薄發按波動率排序 (越低越好)
    res['low_volatility'].sort(key=lambda x: x['volatility_pct'])
    
    market_breadth = 0
    if stat_total > 0: market_breadth = round((stat_new_high / stat_total) * 100, 2)
    
    final = []
    market_date = (datetime.now(tw_tz) - timedelta(days=1)).strftime('%Y-%m-%d') if current_hour < 14 else today_str
    
    if os.path.exists(DATA_FILE):
        try: final = json.load(open(DATA_FILE))
        except: pass
        
    rec = {"date": market_date, "market_breadth": market_breadth, "strategies": res}
    existing_idx = -1
    for i, r in enumerate(final):
        if r['date'] == market_date: existing_idx = i; break
    if existing_idx != -1: final[existing_idx] = rec
    else: final.append(rec)
        
    with open(DATA_FILE, 'w', encoding='utf-8') as f: json.dump(final, f, ensure_ascii=False, indent=2)
    print(f"掃描完成。歸檔日期: {market_date}")

if __name__ == "__main__":
    main()
