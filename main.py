import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import random
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# --- 全域設定 ---
DATA_FILE = "data.json" # 前端讀取的總檔
DATA_DIR = "data"       # 後端儲存的分日資料夾
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

    # 還原權值 Close
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
            
        if fin_data['growth'] and fin_data['growth'] > 0.15:
            score += 1
            reasons.append("(加分) EPS高成長 (+1分)")
        
        if fin_data['pe'] != 999 and fin_data['pe'] < 30:
            score += 1
            reasons.append("(加分) 本益比合理 (+1分)")

        return {"score": score, "reasons": reasons}
    return None

# ==========================================
# 策略 2: 葛蘭碧八大法則 (MA200)
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
# 策略 3: 隔日沖 - 強勢回檔
# ==========================================
def strategy_day_trading(df, ticker, region, latest):
    if len(df) < 50: return None
    ma3 = df['Close'].rolling(window=3).mean().iloc[-1]
    ma4 = df['Close'].rolling(window=4).mean().iloc[-1]
    ma45 = df['Close'].rolling(window=45).mean().iloc[-1]
    ma46 = df['Close'].rolling(window=46).mean().iloc[-1]
    if not (ma3 > ma4 and ma45 > ma46): return None
    
    today = df.iloc[-1]
    if today['Close'] >= today['Open']: return None 
    
    day_prev = df.iloc[-2]; day_prev_2 = df.iloc[-3]
    prev_change = (day_prev['Close'] - day_prev_2['Close']) / day_prev_2['Close']
    if prev_change < 0.095: return None 
    
    if day_prev_2['Close'] <= day_prev_2['Open']: return None 
    
    price_20_ago = df['Close'].iloc[-21]
    rise_20d = (today['Close'] - price_20_ago) / price_20_ago
    if rise_20d <= 0.20: return None
    
    vol = today['Volume']
    if vol < 300000: return None 
    amount = today['Close'] * vol
    if amount < 50000000: return None 
    
    drop_pct = (today['Open'] - today['Close']) / today['Open']
    
    return {
        "drop_pct": round(drop_pct * 100, 2),
        "rise_20d": round(rise_20d * 100, 2),
        "vol_lots": int(vol / 1000),
        "amount_yi": round(amount / 100000000, 2),
        "pattern": "連紅漲停後黑K"
    }

# ==========================================
# 策略 4: 十字星主升起漲
# ==========================================
def strategy_doji_rise(df, ticker, region, latest):
    if len(df) < 65: return None
    close = latest['Close']; open_p = latest['Open']; high_p = latest['High']; low_p = latest['Low']; vol = latest['Volume']
    ma5_vol = df['Volume'].rolling(window=5).mean().iloc[-1]
    ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
    ma60 = df['Close'].rolling(window=60).mean().iloc[-1]
    ma60_prev = df['Close'].rolling(window=60).mean().iloc[-2]
    
    avg_price_5d = df['Close'][-5:].mean()
    avg_value_5d = ma5_vol * avg_price_5d
    if not (ma5_vol >= 5000000 or avg_value_5d >= 1000000000): return None

    if close < ma20 or close < ma60: return None
    if ma60 < ma60_prev: return None

    if close / ma20 > 1.15: return None 

    body_pct = abs(close - open_p) / open_p
    if body_pct > 0.006: return None 
    total_range = high_p - low_p; body_range = abs(close - open_p)
    if total_range < body_range * 2: return None
    if total_range == 0: return None

    vol_ratio = vol / ma5_vol
    if vol_ratio > 1.5: return None
    if vol_ratio < 0.5: return None

    score = 60
    reasons = ["結構+十字星成立 (60分)"]
    if ma5_vol >= 10000000 or avg_value_5d >= 2000000000: score += 5; reasons.append("流動性極佳 (+5)")
    if 0.8 <= vol_ratio <= 1.2: score += 5; reasons.append("量能平穩 (+5)")
    ma5 = df['Close'].rolling(window=5).mean().iloc[-1]
    ma10 = df['Close'].rolling(window=10).mean().iloc[-1]
    if ma5 > ma10 > ma20 > ma60: score += 5; reasons.append("均線多頭排列 (+5)")
    if ma5_vol < 6000000 and avg_value_5d < 1200000000: score -= 10; reasons.append("流動性邊緣 (-10)")
    if vol_ratio > 1.3: score -= 5; reasons.append("量能稍大 (-5)")

    if score < 60: return None
    return {"score": score, "pattern": "標準十字星", "vol_ratio": round(vol_ratio * 100, 1), "vol_avg_val": round(avg_value_5d / 100000000, 1), "trend": "多頭整理", "reasons": reasons}

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
# 策略 6: 厚積薄發 (低波動醞釀)
# ==========================================
def strategy_low_volatility(df, ticker, region, latest):
    if len(df) < 200: return None
    close = latest['Close']; vol = latest['Volume']
    ma50 = df['Close'].rolling(window=50).mean().iloc[-1]
    ma200 = df['Close'].rolling(window=200).mean().iloc[-1]
    vol_ma50 = df['Volume'].rolling(window=50).mean().iloc[-1]
    std_dev_10 = df['Close'].rolling(window=10).std().iloc[-1]
    
    cond_a = close > ma200 and ma50 > ma200
    cond_b = close > ma50
    if not (cond_a and cond_b): return None
    
    volatility_ratio = std_dev_10 / close
    cond_c = volatility_ratio < 0.03
    cond_d = vol > vol_ma50 * 0.7
    
    if cond_c and cond_d:
        return {"tag": "META", "volatility_pct": round(volatility_ratio * 100, 2), "trend_status": "多頭排列 (200MA上)", "volume_status": "量能達標", "desc": "波動壓縮極致(<3%)，且量能維持一定水準，隨時可能發動。"}
    elif volatility_ratio < 0.06:
        return {"tag": "OBSERVE", "volatility_pct": round(volatility_ratio * 100, 2), "trend_status": "多頭排列", "volume_status": "量能待觀察" if not cond_d else "量能達標", "desc": "趨勢正確，但波動尚未收斂至極致(3%~6%)，持續觀察。"}
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
        if res := strategy_low_volatility(df, ticker, region, latest): pkg['low_volatility'] = {**base, **res}; has_res = True
            
        return {"result": pkg if has_res else None, "is_60d_high": is_60d_high}
    except: return None

def main():
    print("啟動全策略掃描 (分檔儲存架構)...")
    
    # 1. 確保資料資料夾存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # 2. 時間判定
    tw_tz = timezone(timedelta(hours=8))
    now = datetime.now(tw_tz)
    today_str = now.strftime('%Y-%m-%d')
    current_hour = now.hour
    
    # 3. 🧹 資料清洗: 掃描 data/ 下的所有檔案，刪除無效日期
    all_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file_path in all_files:
        filename = os.path.basename(file_path)
        file_date = filename.replace(".json", "")
        
        try:
            # 檢查1: 未來日期 -> 刪除
            if file_date > today_str:
                print(f"⚠️ 刪除未來檔案: {filename}")
                os.remove(file_path)
                continue
            
            # 檢查2: 盤中早產兒 -> 刪除 (日期=今天 且 時間<14:00)
            if file_date == today_str and current_hour < 14:
                print(f"⚠️ 刪除盤中未收盤檔案: {filename}")
                os.remove(file_path)
                continue
                
        except:
            pass # 檔名格式不符或其他錯誤忽略

    # 4. 開始掃描當日數據
    stocks = get_tw_stock_list() 
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

    # 排序
    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d'])
    res['doji_rise'].sort(key=lambda x: -x['score'])
    res['low_volatility'].sort(key=lambda x: x['volatility_pct'])
    
    market_breadth = 0
    if stat_total > 0: market_breadth = round((stat_new_high / stat_total) * 100, 2)
    
    # 決定檔案日期 (下午2點前算昨天)
    market_date = (now - timedelta(days=1)).strftime('%Y-%m-%d') if current_hour < 14 else today_str
    
    # 5. 儲存當日單檔 (e.g., data/2025-01-14.json)
    daily_record = {
        "date": market_date,
        "market_breadth": market_breadth,
        "strategies": res
    }
    
    target_file = os.path.join(DATA_DIR, f"{market_date}.json")
    with open(target_file, 'w', encoding='utf-8') as f:
        json.dump(daily_record, f, ensure_ascii=False, indent=2)
    
    print(f"已儲存單日檔案: {target_file}")

    # 6. 合併發布 (Aggregation) -> 生成 data.json 給前端用
    # 重新掃描 data/ 下的所有檔案，按日期排序並合併
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    final_history = []
    
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                final_history.append(data)
        except:
            pass
            
    # 寫入總檔
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_history, f, ensure_ascii=False, indent=2)
        
    print(f"總檔 {DATA_FILE} 更新完成，包含 {len(final_history)} 天份資料。")

if __name__ == "__main__":
    main()
