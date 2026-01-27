import requests # <--- 新增 requests
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

def fetch_data_safe(ticker, retries=3):
    for i in range(retries):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="2y") 
            if not df.empty: return stock, df
        except: time.sleep(1)
    return None, None

def fetch_market_trend():
    print("📈 分析大盤 (0050) RS 基準...")
    try:
        market = yf.Ticker("0050.TW")
        df = market.history(period="3mo")
        if len(df) > 20:
            latest = df['Close'].iloc[-1]
            past_20 = df['Close'].iloc[-21]
            return (latest - past_20) / past_20
    except: pass
    return None

# ==========================================
# [新增] CBAS 可轉債策略模組
# ==========================================
def fetch_active_cbs():
    """抓取櫃買中心可轉債行情"""
    url = "https://www.tpex.org.tw/web/bond/tradeinfo/cb/cb_daily_result.php?l=zh-tw&o=json"
    try:
        print("🔗 連線櫃買中心抓取 CB 資料...")
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
        raw_list = data.get('aaData', [])
        
        cb_list = []
        for row in raw_list:
            try:
                cb_id = row[0]
                cb_name = row[1]
                
                # 數值處理
                def parse_float(val):
                    if isinstance(val, str):
                        val = val.replace(',', '')
                        if '--' in val or val.strip() == '': return None
                    return float(val)

                cb_close = parse_float(row[3])
                conv_price = parse_float(row[12])
                
                if cb_close is None or conv_price is None: continue

                # CB代號前4碼通常是股票代號
                stock_id = cb_id[:4]
                cb_list.append({
                    "stock_id": stock_id,
                    "cb_id": cb_id,
                    "cb_name": cb_name,
                    "cb_price": cb_close,
                    "conversion_price": conv_price
                })
            except: continue
        return cb_list
    except Exception as e:
        print(f"⚠️ CB 資料抓取失敗: {e}")
        return []

def check_cbas_signal(stock_id):
    """檢查個股是否符合發動訊號 (突破上軌 + 出量)"""
    suffixes = ['.TW', '.TWO']
    df = None; valid_symbol = None
    
    for suffix in suffixes:
        symbol = f"{stock_id}{suffix}"
        # 重用 fetch_data_safe (這裡用它抓資料很方便)
        _, tmp_df = fetch_data_safe(symbol, retries=1)
        if tmp_df is not None and len(tmp_df) > 30:
            df = tmp_df
            valid_symbol = symbol
            break
            
    if df is None: return None

    # 技術指標計算
    close = df['Close']
    volume = df['Volume']
    
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper = ma20 + (2 * std20)
    vol_ma5 = volume.rolling(5).mean()
    
    curr_close = close.iloc[-1]
    curr_vol = volume.iloc[-1]
    curr_upper = upper.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    prev_close = close.iloc[-2]

    # 1. 突破條件: 突破布林上軌
    is_breakout = curr_close > curr_upper
    
    # 2. 量能條件: 大於 2 倍均量
    is_volume_surge = curr_vol > (curr_vol_ma5 * 2.0)
    
    if is_breakout and is_volume_surge:
        # 取得股票名稱
        stock_name = get_stock_name(valid_symbol, "TW")
        pct_change = round(((curr_close - prev_close) / prev_close) * 100, 2)
        
        return {
            "code": valid_symbol,
            "name": stock_name,
            "price": float(f"{curr_close:.2f}"),
            "pct_change": pct_change,
            "vol_ratio": round(curr_vol / curr_vol_ma5, 1) if curr_vol_ma5 > 0 else 0
        }
    return None

def run_cbas_scanner():
    """執行完整的 CBAS 掃描流程"""
    print("🚀 啟動 CBAS (可轉債發動) 掃描...")
    cb_list = fetch_active_cbs()
    if not cb_list: return []
    
    unique_stocks = list(set([item['stock_id'] for item in cb_list]))
    print(f"   共監控 {len(cb_list)} 檔可轉債，關聯 {len(unique_stocks)} 檔個股")
    
    stock_signals = {}
    
    # 多線程掃描個股訊號
    with ThreadPoolExecutor(max_workers=10) as exc:
        future_to_sid = {exc.submit(check_cbas_signal, sid): sid for sid in unique_stocks}
        for future in as_completed(future_to_sid):
            res = future.result()
            if res:
                # 只取代碼前4碼做 key
                sid = res['code'].split('.')[0]
                stock_signals[sid] = res
    
    # 整合結果 (計算雙低)
    results = []
    for cb in cb_list:
        sid = cb['stock_id']
        if sid in stock_signals:
            sig = stock_signals[sid]
            
            # 雙低公式
            parity = (sig['price'] / cb['conversion_price']) * 100
            premium = ((cb['cb_price'] - parity) / parity) * 100
            double_low = cb['cb_price'] + premium
            
            results.append({
                "code": sig['code'],
                "name": sig['name'],
                "price": sig['price'],
                "pct_change": sig['pct_change'],
                "cb_name": cb['cb_name'],
                "cb_price": cb['cb_price'],
                "premium_pct": round(premium, 2),
                "double_low": round(double_low, 2),
                "desc": f"CB:{cb['cb_name']} | 雙低:{round(double_low, 2)}"
            })
            
    # 依照雙低排序 (越低越好)
    results.sort(key=lambda x: x['double_low'])
    print(f"✅ CBAS 掃描完成，找到 {len(results)} 檔標的")
    return results

# ==========================================
# 既有策略群
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

def strategy_low_volatility(df, ticker, region, latest, market_ret_20d):
    if len(df) < 205: return None
    
    close_s = df['Close']; vol_s = df['Volume']
    ma200 = close_s.rolling(window=200, min_periods=150).mean()
    curr_ma200 = float(ma200.iloc[-1]); prev_ma200 = float(ma200.iloc[-2])
    ma20 = close_s.rolling(window=20, min_periods=15).mean()
    vol_ma50 = vol_s.rolling(window=50, min_periods=40).mean()
    std_20 = close_s.rolling(window=20, min_periods=15).std()
    curr_close = float(close_s.iloc[-1]); prev_close = float(close_s.iloc[-2])
    curr_vol = float(vol_s.iloc[-1]); curr_ma20 = float(ma20.iloc[-1])
    curr_vol_ma50 = float(vol_ma50.iloc[-1]); curr_std_20 = float(std_20.iloc[-1])

    if pd.isna(curr_ma200): return None

    # Step 1: Granville Filter
    granville_type = None
    if curr_ma200 <= prev_ma200: return None
    if prev_close < prev_ma200 and curr_close > curr_ma200:
        granville_type = "法則二 (假跌破)"
    elif curr_close > curr_ma200:
        dist = (df['Low'].iloc[-1] - curr_ma200) / curr_ma200
        if 0 <= dist < 0.015 and curr_close > df['Open'].iloc[-1]:
             granville_type = "法則三 (回測支撐)"
    
    if not granville_type: return None

    # Step 2: Scoring
    score = 0; signals = []
    if pd.notna(curr_std_20) and curr_ma20 > 0:
        if (4 * curr_std_20) / curr_ma20 < 0.10: score += 1; signals.append("布林壓縮")
    if pd.notna(curr_vol_ma50) and curr_vol_ma50 > 0:
        if curr_vol < (curr_vol_ma50 * 0.5): score += 1; signals.append("量能急凍")
    if market_ret_20d is not None and len(close_s) > 22:
        price_20_ago = float(close_s.iloc[-21])
        if price_20_ago > 0:
            if (curr_close - price_20_ago) / price_20_ago > market_ret_20d: score += 1; signals.append("相對強勢")

    tag = f"★ {score}分"
    if score == 0: tag = "觀察 (0分)"
    if score == 3: tag = "★ 3分 (滿分)"
    
    desc_text = f"【{granville_type}】"
    if signals: desc_text += " + " + " | ".join(signals)
    else: desc_text += " (符合葛蘭碧買點)"

    vol_pct = 0
    std_10 = close_s.rolling(window=10, min_periods=5).std().iloc[-1]
    if pd.notna(std_10) and curr_close > 0: vol_pct = round((std_10 / curr_close) * 100, 2)

    return {
        "tag": tag, "volatility_pct": vol_pct, "trend_status": f"MA200上揚 ({granville_type})",
        "volume_status": "量能收縮" if (pd.notna(curr_vol_ma50) and curr_vol < curr_vol_ma50) else "量能放大",
        "desc": desc_text, "score_val": score
    }

def analyze_stock(stock_info, market_ret_20d):
    ticker = stock_info['code']
    region = stock_info['region']
    stock, df = fetch_data_safe(ticker)
    
    if stock is None or df is None or len(df) < 205: return None
        
    latest = df.iloc[-1]; prev = df.iloc[-2]
    real_trade_date = latest.name.strftime('%Y-%m-%d')
    window_high_short = df['Close'][-61:-1].max()
    is_60d_high = latest['Close'] > window_high_short
    fin_data = get_financial_details(stock)
    display_name = get_stock_name(ticker, region, stock)
    
    base = {"code": ticker, "name": display_name, "region": region, "price": float(f"{latest['Close']:.2f}"), "date": real_trade_date, "fundamentals": fin_data}
    pkg = {}; has_res = False
    
    if res := strategy_momentum(df, ticker, region, latest, prev, fin_data): pkg['momentum'] = {**base, **res}; has_res = True
    if res := strategy_day_trading(df, ticker, region, latest): pkg['day_trading'] = {**base, **res}; has_res = True
    if res := strategy_doji_rise(df, ticker, region, latest): pkg['doji_rise'] = {**base, **res}; has_res = True
    if res := strategy_active_etf(ticker, latest['Close']): pkg['active_etf'] = {**base, **res}; has_res = True
    if res := strategy_low_volatility(df, ticker, region, latest, market_ret_20d): pkg['low_volatility'] = {**base, **res}; has_res = True
        
    return {"result": pkg if has_res else None, "is_60d_high": is_60d_high, "trade_date": real_trade_date}

def main():
    print("啟動全策略掃描 (含 CBAS 新策略)...")
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        
    all_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file_path in all_files:
        filename = os.path.basename(file_path)
        file_date_str = filename.replace(".json", "")
        try:
            file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
            if file_date.weekday() >= 5: os.remove(file_path)
        except: pass

    tw_tz = timezone(timedelta(hours=8))
    now = datetime.now(tw_tz)
    expected_date = now.strftime('%Y-%m-%d')
    if now.hour < 14: expected_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    exp_dt = datetime.strptime(expected_date, '%Y-%m-%d')
    if exp_dt.weekday() == 6: expected_date = (exp_dt - timedelta(days=2)).strftime('%Y-%m-%d')
    elif exp_dt.weekday() == 5: expected_date = (exp_dt - timedelta(days=1)).strftime('%Y-%m-%d')

    market_ret_20d = fetch_market_trend()
    stocks = get_tw_stock_list() 
    
    # 1. 執行 CBAS 掃描 (獨立執行)
    cbas_results = run_cbas_scanner()
    
    # 2. 執行一般個股掃描
    res = {"momentum": [], "day_trading": [], "doji_rise": [], "active_etf": [], "low_volatility": []}
    stat_total = 0; stat_new_high = 0; detected_market_date = None
    
    with ThreadPoolExecutor(max_workers=20) as exc:
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

    # 將 CBAS 結果加入總表 (使用新 Key 'cbas')
    res['cbas'] = clean_for_json(cbas_results)

    if detected_market_date and detected_market_date != expected_date:
        print(f"⚠️ [警告] 日期不符 ({detected_market_date} vs {expected_date})")

    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d'])
    res['doji_rise'].sort(key=lambda x: -x['score'])
    res['low_volatility'].sort(key=lambda x: -x.get('score_val', 0))
    
    market_breadth = 0
    if stat_total > 0: market_breadth = round((stat_new_high / stat_total) * 100, 2)
    
    final_date = detected_market_date if detected_market_date else expected_date
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
