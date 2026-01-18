import yfinance as yf
import pandas as pd
import twstock
import json
import os
import glob
import random
import math
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# --- 全域設定 ---
DATA_FILE = "data.json"
DATA_DIR = "data"
tw_stock_map = twstock.codes 

# --- 模擬資料 ---
MOCK_ETF_DB = {
    "00980A": {"name": "野村台灣創新", "holdings": {"2330.TW": {"shares": 500, "pct": 15.2}, "2317.TW": {"shares": 300, "pct": 8.5}, "2454.TW": {"shares": 100, "pct": 5.1}}},
    "00981A": {"name": "凱基優選", "holdings": {"2330.TW": {"shares": 800, "pct": 18.1}, "2303.TW": {"shares": 1200, "pct": 6.2}, "2603.TW": {"shares": 500, "pct": 4.3}}},
    "00982A": {"name": "富邦成長", "holdings": {"2330.TW": {"shares": 600, "pct": 12.0}, "2317.TW": {"shares": 400, "pct": 7.8}, "3008.TW": {"shares": 50, "pct": 3.2}}},
}

# --- 工具函式 ---
def clean_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj

def get_stock_name(ticker, region, stock_obj=None):
    display_name = ticker
    if region == 'TW':
        clean_code = ticker.split('.')[0]
        if clean_code in tw_stock_map: return tw_stock_map[clean_code].name
    if stock_obj:
        try: return stock_obj.info.get('longName') or stock_obj.info.get('shortName') or ticker
        except: pass
    return display_name

def get_tw_stock_list():
    stocks = []
    for code in twstock.twse:
        if len(code) == 4: stocks.append({"code": f"{code}.TW", "region": "TW"})
    for code in twstock.tpex:
        if len(code) == 4: stocks.append({"code": f"{code}.TWO", "region": "TW"})
    return stocks

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
                curr = vals[i]; qoq = None
                if i+1 < len(vals) and vals[i+1] != 0: qoq = (curr - vals[i+1]) / vals[i+1]
                data['quarters'].append({"date": vals.index[i].strftime('%Y-%m'), "revenue": curr, "qoq": qoq})
    except: pass
    return data

# --- 安全抓取函式 (Retry) ---
def fetch_data_safe(ticker, retries=3):
    for i in range(retries):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="2y") 
            if not df.empty: return stock, df
        except:
            time.sleep(1)
    return None, None

# --- [新增] 抓取大盤趨勢 (用於相對強勢 RS) ---
def fetch_market_trend():
    print("📈 正在分析大盤 (0050) 趨勢，以計算相對強勢(RS)...")
    try:
        market = yf.Ticker("0050.TW")
        df = market.history(period="3mo")
        if len(df) > 20:
            latest = df['Close'].iloc[-1]
            past_20 = df['Close'].iloc[-21]
            # 計算大盤近20日漲跌幅
            market_return_20d = (latest - past_20) / past_20
            print(f"   大盤近20日漲幅: {market_return_20d*100:.2f}%")
            return market_return_20d
    except Exception as e:
        print(f"   ⚠️ 大盤資料抓取失敗 ({e})，將跳過 RS 判斷。")
    return None

# ==========================================
# 策略 1~5 (保持不變)
# ==========================================
def strategy_momentum(df, ticker, region, latest, prev, fin_data):
    LOOKBACK_SHORT = 60; LOOKBACK_LONG = 500; VOL_FACTOR = 1.2; GROWTH_REV_PRIORITY = 0.15
    if latest['Volume'] < (500000 if region == 'TW' else 1000000): return None
    window_high_short = df['Close'][-LOOKBACK_SHORT-1:-1].max()
    is_new_high = latest['Close'] > window_high_short
    was_high_yesterday = prev['Close'] > window_high_short
    if is_new_high and not was_high_yesterday:
        score = 3; reasons = ["(基礎) 創季新高 +3分"]
        vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
        if latest['Volume'] > vol_ma20 * VOL_FACTOR: reasons.append(f"(基礎) 量增{VOL_FACTOR}倍")
        if latest['Close'] > df['Close'][-LOOKBACK_LONG-1:-1].max(): score += 2; reasons.append("(加分) 兩年新高 +2分")
        if fin_data['rev_yoy'] and fin_data['rev_yoy'] > GROWTH_REV_PRIORITY: score += 3; reasons.append("★營收年增>15% (+3分)")
        elif fin_data['rev_yoy'] and fin_data['rev_yoy'] > 0: score += 1; reasons.append("(加分) 營收正成長 (+1分)")
        if fin_data['growth'] and fin_data['growth'] > 0.15: score += 1; reasons.append("(加分) EPS高成長 (+1分)")
        if fin_data['pe'] != 999 and fin_data['pe'] < 30: score += 1; reasons.append("(加分) 本益比合理 (+1分)")
        return {"score": score, "reasons": reasons}
    return None

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
# 策略 6: 厚積薄發 (V3 - 究極進化版)
# ==========================================
def strategy_low_volatility(df, ticker, region, latest, market_ret_20d):
    if len(df) < 205: return None
    
    close_series = df['Close']
    vol_series = df['Volume']
    high_series = df['High']
    low_series = df['Low']
    
    # 均線
    ma20 = close_series.rolling(window=20, min_periods=15).mean()
    ma50 = close_series.rolling(window=50, min_periods=40).mean()
    ma200 = close_series.rolling(window=200, min_periods=150).mean()
    vol_ma50 = vol_series.rolling(window=50, min_periods=40).mean()
    
    # 布林通道 (Bollinger Bands) - 用於判斷壓縮
    std_20 = close_series.rolling(window=20, min_periods=15).std()
    
    # 波動率 (顯示用)
    std_10 = close_series.rolling(window=10, min_periods=5).std().iloc[-1]
    
    curr_close = float(close_series.iloc[-1])
    curr_open = float(df['Open'].iloc[-1])
    curr_low = float(low_series.iloc[-1])
    curr_vol = float(vol_series.iloc[-1])
    
    curr_ma20 = float(ma20.iloc[-1])
    curr_ma50 = float(ma50.iloc[-1])
    curr_ma200 = float(ma200.iloc[-1])
    curr_vol_ma50 = float(vol_ma50.iloc[-1])
    curr_std_20 = float(std_20.iloc[-1])
    prev_high = float(high_series.iloc[-2])

    # 防呆
    if pd.isna(curr_ma50) or pd.isna(curr_ma200): return None

    # --- 1. 核心趨勢 (Core Trend) ---
    cond_trend = (curr_close > curr_ma200) and (curr_ma50 > curr_ma200)
    cond_support = (curr_close > curr_ma50)
    
    if not (cond_trend and cond_support): return None 

    # --- 2. 訊號偵測 (Signals) ---
    signals = []
    
    # A. 縮量十字星
    body_size = abs(curr_close - curr_open)
    is_doji = body_size < (curr_close * 0.005)
    if is_doji: signals.append("★ 十字星")

    # B. 強力跳空
    if curr_low > prev_high: signals.append("★ 強力跳空")

    # C. 50MA 完美回測
    dist_to_ma50 = (curr_close - curr_ma50) / curr_ma50
    if 0 <= dist_to_ma50 < 0.03: signals.append("★ 50MA 回測")

    # D. [新增] 布林通道壓縮 (BB Squeeze)
    # 帶寬 = (上軌 - 下軌) / 中軌 = (4 * std) / ma20
    if pd.notna(curr_std_20) and curr_ma20 > 0:
        bb_width = (4 * curr_std_20) / curr_ma20
        if bb_width < 0.10: # 壓縮在 10% 以內
            signals.append("★ 布林壓縮")

    # E. [新增] 量能急凍 (Volume Dry-up)
    if pd.notna(curr_vol_ma50) and curr_vol_ma50 > 0:
        if curr_vol < (curr_vol_ma50 * 0.5): # 量縮到均量的一半以下
            signals.append("★ 量能急凍")

    # F. [新增] 相對強勢 (Relative Strength)
    if market_ret_20d is not None and len(close_series) > 22:
        price_20_ago = float(close_series.iloc[-21])
        if price_20_ago > 0:
            stock_ret_20d = (curr_close - price_20_ago) / price_20_ago
            if stock_ret_20d > market_ret_20d:
                signals.append("★ 相對強勢")

    # --- 3. 輸出結果 ---
    tag = "OBSERVE"
    desc_text = "趨勢多頭 (觀察中)"
    
    if signals:
        tag = "META"
        desc_text = " | ".join(signals)
        
    vol_pct = 0
    if pd.notna(std_10) and curr_close > 0:
        vol_pct = round((std_10 / curr_close) * 100, 2)

    return {
        "tag": tag,
        "volatility_pct": vol_pct,
        "trend_status": "多頭排列",
        "volume_status": "量能收縮" if (pd.notna(curr_vol_ma50) and curr_vol < curr_vol_ma50) else "量能放大",
        "desc": desc_text
    }

def analyze_stock(stock_info, market_ret_20d):
    ticker = stock_info['code']
    region = stock_info['region']
    
    stock, df = fetch_data_safe(ticker)
    
    if stock is None or df is None or len(df) < 205: return None
        
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    real_trade_date = latest.name.strftime('%Y-%m-%d')
    window_high_short = df['Close'][-61:-1].max()
    is_60d_high = latest['Close'] > window_high_short
    fin_data = get_financial_details(stock)
    display_name = get_stock_name(ticker, region, stock)
    
    base = {"code": ticker, "name": display_name, "region": region, "price": float(f"{latest['Close']:.2f}"), "date": real_trade_date, "fundamentals": fin_data}
    pkg = {}; has_res = False
    
    if res := strategy_momentum(df, ticker, region, latest, prev, fin_data): pkg['momentum'] = {**base, **res}; has_res = True
    if res := strategy_granville(df, ticker, region, latest, prev): pkg['granville'] = {**base, **res}; has_res = True
    if res := strategy_day_trading(df, ticker, region, latest): pkg['day_trading'] = {**base, **res}; has_res = True
    if res := strategy_doji_rise(df, ticker, region, latest): pkg['doji_rise'] = {**base, **res}; has_res = True
    if res := strategy_active_etf(ticker, latest['Close']): pkg['active_etf'] = {**base, **res}; has_res = True
    
    # 傳入 market_ret_20d 進行比較
    if res := strategy_low_volatility(df, ticker, region, latest, market_ret_20d): pkg['low_volatility'] = {**base, **res}; has_res = True
        
    return {"result": pkg if has_res else None, "is_60d_high": is_60d_high, "trade_date": real_trade_date}

def main():
    print("啟動全策略掃描 (V3 究極進化版 - 含RS/布林/量縮)...")
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        
    all_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file_path in all_files:
        filename = os.path.basename(file_path)
        file_date_str = filename.replace(".json", "")
        try:
            file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
            if file_date.weekday() >= 5: os.remove(file_path)
        except: pass

    # 1. 先抓大盤 RS 基準
    market_ret_20d = fetch_market_trend()

    stocks = get_tw_stock_list() 
    res = {"momentum": [], "granville_buy": [], "granville_sell": [], "day_trading": [], "doji_rise": [], "active_etf": [], "low_volatility": []}
    stat_total = 0; stat_new_high = 0; detected_market_date = None
    
    with ThreadPoolExecutor(max_workers=20) as exc:
        # 將 market_ret_20d 傳入每個執行緒
        futures = [exc.submit(analyze_stock, s, market_ret_20d) for s in stocks]
        for f in as_completed(futures):
            ret = f.result()
            if ret:
                if detected_market_date is None and ret.get("trade_date"): detected_market_date = ret["trade_date"]
                stat_total += 1
                if ret['is_60d_high']: stat_new_high += 1
                if r := ret['result']:
                    for k in res.keys():
                        if k in r: res[k].append(r[k])

    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d'])
    res['doji_rise'].sort(key=lambda x: -x['score'])
    # 新排序邏輯：有 META 的排前面，再來比波動率低
    res['low_volatility'].sort(key=lambda x: (0 if x['tag'] == 'META' else 1, x['volatility_pct']))
    
    market_breadth = 0
    if stat_total > 0: market_breadth = round((stat_new_high / stat_total) * 100, 2)
    
    final_date = detected_market_date if detected_market_date else datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    print(f"✅ 確認歸檔日期: {final_date}")
    
    daily_record = clean_for_json({"date": final_date, "market_breadth": market_breadth, "strategies": res})
    with open(os.path.join(DATA_DIR, f"{final_date}.json"), 'w', encoding='utf-8') as f:
        json.dump(daily_record, f, ensure_ascii=False, indent=2)
    
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    final_history = []
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                d = datetime.strptime(data['date'], '%Y-%m-%d')
                if d.weekday() < 5: final_history.append(data)
        except: pass
            
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(clean_for_json(final_history), f, ensure_ascii=False, indent=2)
    print(f"總檔更新完成。日期: {final_date} / 新高佔比: {market_breadth}%")

if __name__ == "__main__":
    main()
