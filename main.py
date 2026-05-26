import requests
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

TWSE_FUND_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap47_L"
TWSE_QUOTE_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_CB_ISSUE_URL = "https://www.tpex.org.tw/openapi/v1/bond_ISSBD5_data"
TPEX_CB_QUOTE_URL = "https://www.tpex.org.tw/www/zh-tw/bond/cbDayQry"

# --- 工具函式 ---
def fetch_json(url, params=None, timeout=20):
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, params=params, headers=headers, timeout=timeout)
    res.raise_for_status()
    return json.loads(res.content.decode("utf-8-sig"))

def parse_float(val):
    if val is None: return None
    if isinstance(val, (int, float)): return float(val)
    text = str(val).replace(",", "").strip()
    if text == "" or text in {"--", "-"}: return None
    try: return float(text)
    except ValueError: return None

def roc_or_yyyymmdd_to_iso(val):
    if not val: return None
    text = str(val).strip().replace("/", "").replace("-", "")
    if len(text) == 7 and text.isdigit():
        year = int(text[:3]) + 1911
        return f"{year:04d}-{text[3:5]}-{text[5:7]}"
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return str(val)

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

def get_tw_ticker_candidates(stock_id):
    if stock_id in twstock.twse:
        return [f"{stock_id}.TW"]
    if stock_id in twstock.tpex:
        return [f"{stock_id}.TWO"]
    return [f"{stock_id}.TW", f"{stock_id}.TWO"]

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

# ==========================================
# CBAS 可轉債策略模組
# ==========================================
def fetch_active_cbs():
    try:
        print("連線 TPEx OpenAPI 抓取可轉債發行資料...")
        raw_list = fetch_json(TPEX_CB_ISSUE_URL)
        cb_list = []
        for row in raw_list:
            try:
                cb_id = (row.get("BondCode") or "").strip()
                stock_id = (row.get("IssuerCode") or "").strip()
                conv_price = parse_float(row.get("Conversion/ExchangePriceAtIssuance"))
                is_listed = row.get("ListingStatus") == "2"
                is_public = row.get("OfferingMethod") == "7"
                if not (cb_id and len(stock_id) == 4 and stock_id.isdigit() and is_listed and is_public):
                    continue
                if conv_price is None or conv_price <= 0:
                    continue
                cb_list.append({
                    "stock_id": stock_id,
                    "cb_id": cb_id,
                    "cb_name": row.get("ShortName") or cb_id,
                    "conversion_price": conv_price,
                    "issue_date": roc_or_yyyymmdd_to_iso(row.get("IssueDate")),
                    "maturity_date": roc_or_yyyymmdd_to_iso(row.get("MaturityDate")),
                    "listing_date": roc_or_yyyymmdd_to_iso(row.get("ListingDate")),
                    "outstanding_amount": parse_float(row.get("OutstandingAmount")),
                    "put_option_date": roc_or_yyyymmdd_to_iso(row.get("PutOptionDate")),
                    "put_option_price": parse_float(row.get("PutOptionPrice")),
                    "guaranteed": row.get("Guaranteed") == "1",
                })
            except: continue
        print(f"可轉債發行資料：{len(cb_list)} 檔")
        return cb_list
    except Exception as e:
        print(f"CB 資料抓取失敗: {e}")
        return []

def fetch_cb_latest_quote(cb_id):
    try:
        data = fetch_json(TPEX_CB_QUOTE_URL, params={"code": cb_id, "response": "json"}, timeout=15)
        tables = data.get("tables") or []
        rows = tables[0].get("data", []) if tables else []
        latest = None
        for row in rows:
            if len(row) < 11 or row[1] != "等價":
                continue
            close = parse_float(row[2])
            if close is None:
                continue
            latest = {
                "trade_date": roc_or_yyyymmdd_to_iso(row[0]),
                "cb_price": close,
                "cb_change": parse_float(row[3]),
                "cb_open": parse_float(row[4]),
                "cb_high": parse_float(row[5]),
                "cb_low": parse_float(row[6]),
                "cb_transactions": parse_float(row[7]),
                "cb_units": parse_float(row[8]),
                "cb_trade_value": parse_float(row[9]),
                "cb_avg_price": parse_float(row[10]),
            }
        return latest
    except Exception:
        return None

def check_cbas_signal(stock_id):
    df = None; valid_symbol = None
    for symbol in get_tw_ticker_candidates(stock_id):
        _, tmp_df = fetch_data_safe(symbol, retries=1)
        if tmp_df is not None and len(tmp_df) > 30:
            df = tmp_df; valid_symbol = symbol; break
            
    if df is None: return None

    close = df['Close']; volume = df['Volume']
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper = ma20 + (2 * std20)
    vol_ma5 = volume.rolling(5).mean()
    
    curr_close = close.iloc[-1]; curr_vol = volume.iloc[-1]
    curr_upper = upper.iloc[-1]; curr_vol_ma5 = vol_ma5.iloc[-1]
    prev_close = close.iloc[-2]

    # 策略: 突破上軌 + 量增 (CBAS)
    is_breakout = curr_close > curr_upper
    is_volume_surge = curr_vol > (curr_vol_ma5 * 2.0)
    
    if is_breakout and is_volume_surge:
        stock_name = get_stock_name(valid_symbol, "TW")
        pct_change = round(((curr_close - prev_close) / prev_close) * 100, 2)
        return {"code": valid_symbol, "name": stock_name, "price": float(f"{curr_close:.2f}"), "pct_change": pct_change, "vol_ratio": round(curr_vol / curr_vol_ma5, 1) if curr_vol_ma5 > 0 else 0}
    return None

def run_cbas_scanner():
    print("啟動 CBAS (可轉債發動) 掃描...")
    cb_list = fetch_active_cbs()
    if not cb_list: return []
    
    unique_stocks = list(set([item['stock_id'] for item in cb_list]))
    stock_signals = {}
    
    with ThreadPoolExecutor(max_workers=10) as exc:
        future_to_sid = {exc.submit(check_cbas_signal, sid): sid for sid in unique_stocks}
        for future in as_completed(future_to_sid):
            res = future.result()
            if res: stock_signals[res['code'].split('.')[0]] = res
    
    results = []
    for cb in cb_list:
        sid = cb['stock_id']
        if sid in stock_signals:
            sig = stock_signals[sid]
            quote = fetch_cb_latest_quote(cb['cb_id'])
            if not quote:
                continue
            parity = (sig['price'] / cb['conversion_price']) * 100
            if parity <= 0:
                continue
            premium = ((quote['cb_price'] - parity) / parity) * 100
            double_low = quote['cb_price'] + premium
            results.append({
                "code": sig['code'], "name": sig['name'], "price": sig['price'], "pct_change": sig['pct_change'],
                "cb_code": cb['cb_id'], "cb_name": cb['cb_name'], "cb_price": quote['cb_price'],
                "conversion_price": cb['conversion_price'], "conversion_value": round(parity, 2),
                "premium_pct": round(premium, 2), "double_low": round(double_low, 2),
                "cb_trade_date": quote.get("trade_date"), "cb_units": quote.get("cb_units"),
                "cb_trade_value": quote.get("cb_trade_value"),
                "maturity_date": cb.get("maturity_date"), "put_option_date": cb.get("put_option_date"),
                "put_option_price": cb.get("put_option_price"), "guaranteed": cb.get("guaranteed"),
                "desc": f"CB:{cb['cb_name']} | 雙低:{round(double_low, 2)}"
            })
            
    results.sort(key=lambda x: x['double_low'])
    print(f"CBAS 掃描完成，找到 {len(results)} 檔標的")
    return results

# ==========================================
# 既有策略群 (移除厚積薄發)
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

def fetch_twse_quote_map():
    try:
        rows = fetch_json(TWSE_QUOTE_URL)
        return {row.get("Code"): row for row in rows if row.get("Code")}
    except Exception as e:
        print(f"TWSE 行情資料抓取失敗: {e}")
        return {}

def fetch_active_etfs():
    try:
        funds = fetch_json(TWSE_FUND_URL)
        quotes = fetch_twse_quote_map()
        results = []
        for fund in funds:
            fund_type = fund.get("基金類型") or ""
            full_name = fund.get("基金中文名稱") or ""
            if "主動式交易所交易基金" not in fund_type and "主動式ETF" not in full_name:
                continue

            code = fund.get("基金代號")
            quote = quotes.get(code, {})
            close = parse_float(quote.get("ClosingPrice"))
            change = parse_float(quote.get("Change"))
            prev_close = close - change if close is not None and change is not None else None
            pct_change = round((change / prev_close) * 100, 2) if prev_close else None
            trade_value = parse_float(quote.get("TradeValue"))

            results.append({
                "code": code,
                "name": fund.get("基金簡稱") or code,
                "full_name": full_name,
                "price": close,
                "change": change,
                "pct_change": pct_change,
                "date": roc_or_yyyymmdd_to_iso(quote.get("Date") or fund.get("出表日期")),
                "fund_type": fund_type,
                "market": "國內" if "國內" in fund_type else "國外",
                "asset_class": "債券" if "債券" in fund_type else "股票",
                "listing_date": roc_or_yyyymmdd_to_iso(fund.get("上市日期")),
                "manager": fund.get("基金經理人"),
                "benchmark": fund.get("績效指標中文名稱") or fund.get("標的指數/追蹤指數名稱"),
                "includes_foreign": fund.get("是否包含國外成分股"),
                "issued_units": parse_float(fund.get("發行單位數/轉換數")),
                "trade_volume": parse_float(quote.get("TradeVolume")),
                "trade_value": trade_value,
                "source": "TWSE OpenAPI",
            })
        results.sort(key=lambda x: x.get("trade_value") or 0, reverse=True)
        print(f"主動式 ETF 資料：{len(results)} 檔")
        return results
    except Exception as e:
        print(f"主動式 ETF 資料抓取失敗: {e}")
        return []

def analyze_stock(stock_info):
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
    # Low Volatility 已移除
        
    return {"result": pkg if has_res else None, "is_60d_high": is_60d_high, "trade_date": real_trade_date}

def main():
    print("啟動全策略掃描 (Clean版 + CBAS)...")
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        
    all_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file_path in all_files:
        filename = os.path.basename(file_path)
        file_date_str = filename.replace(".json", "")
        try:
            file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
            if file_date.weekday() >= 5: os.remove(file_path)
        except: pass

    # 日期檢查
    tw_tz = timezone(timedelta(hours=8))
    now = datetime.now(tw_tz)
    expected_date = now.strftime('%Y-%m-%d')
    if now.hour < 14: expected_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    exp_dt = datetime.strptime(expected_date, '%Y-%m-%d')
    if exp_dt.weekday() == 6: expected_date = (exp_dt - timedelta(days=2)).strftime('%Y-%m-%d')
    elif exp_dt.weekday() == 5: expected_date = (exp_dt - timedelta(days=1)).strftime('%Y-%m-%d')

    stocks = get_tw_stock_list() 
    
    # 1. 執行 CBAS 掃描
    cbas_results = run_cbas_scanner()
    
    # 2. 執行一般個股掃描
    res = {"momentum": [], "day_trading": [], "doji_rise": [], "active_etf": []}
    stat_total = 0; stat_new_high = 0; detected_market_date = None
    
    with ThreadPoolExecutor(max_workers=20) as exc:
        futures = [exc.submit(analyze_stock, s) for s in stocks]
        for f in as_completed(futures):
            ret = f.result()
            if ret:
                if detected_market_date is None and ret.get("trade_date"): detected_market_date = ret["trade_date"]
                stat_total += 1
                if ret['is_60d_high']: stat_new_high += 1
                if r := ret['result']:
                    for k in res.keys():
                        if k in r: res[k].append(r[k])

    res['cbas'] = clean_for_json(cbas_results)
    res['active_etf'] = clean_for_json(fetch_active_etfs())

    if detected_market_date and detected_market_date != expected_date:
        print(f"[警告] 日期不符 ({detected_market_date} vs {expected_date})")

    res['momentum'].sort(key=lambda x: -x['score'])
    res['day_trading'].sort(key=lambda x: -x['rise_20d'])
    res['doji_rise'].sort(key=lambda x: -x['score'])
    
    market_breadth = 0
    if stat_total > 0: market_breadth = round((stat_new_high / stat_total) * 100, 2)
    
    final_date = detected_market_date if detected_market_date else expected_date
    print(f"確認歸檔日期: {final_date}")
    
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
