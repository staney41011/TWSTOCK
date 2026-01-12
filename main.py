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

# --- 模擬資料：主動式 ETF 持股庫 (因為目前無免費API可抓即時明細) ---
# 您未來可以透過爬蟲更新此字典，格式：ETF代號 -> {股票代號: {張數, 佔比%}}
# 這裡先用模擬數據展示功能
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
    # 範例只抓上市櫃部分熱門股以節省時間，實際可全抓
    for code in twstock.twse:
        if len(code) == 4: stocks.append({"code": f"{code}.TW", "region": "TW"})
    for code in twstock.tpex:
        if len(code) == 4: stocks.append({"code": f"{code}.TWO", "region": "TW"})
    return stocks

def get_us_stock_list():
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = table[0]
        symbols = df['Symbol'].values.tolist()
        return [{"code": s.replace('.', '-'), "region": "US"} for s in symbols]
    except:
        return []

# ==========================================
# 策略 1: 動能爆發 (Momentum) - 維持原樣
# ==========================================
def strategy_momentum(df, ticker, region, latest, prev, fin_data):
    # 參數
    LOOKBACK_SHORT = 60
    LOOKBACK_LONG = 500
    VOL_FACTOR = 1.2
    GROWTH_REV_PRIORITY = 0.15

    # 1. 量能濾網
    min_vol = 500000 if region == 'TW' else 1000000
    if latest['Volume'] < min_vol: return None

    # 2. 創新高判斷
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
        
        return {"score": score, "reasons": reasons}
    return None

# ==========================================
# 策略 2: 葛蘭碧八大法則 (Granville MA200)
# ==========================================
def strategy_granville(df, ticker, region, latest, prev):
    # 至少需要 200 天資料
    if len(df) < 205: return None
    
    # 計算 MA200
    ma200 = df['Close'].rolling(window=200).mean()
    curr_ma = ma200.iloc[-1]
    prev_ma = ma200.iloc[-2]
    
    # 判斷均線趨勢 (Slope)
    ma_rising = curr_ma > prev_ma
    ma_falling = curr_ma < prev_ma
    
    close = latest['Close']
    prev_close = prev['Close']
    
    result = None
    
    # --- 進場訊號 (買進) ---
    
    # 法則 2: 假跌破 (股價跌破 MA，但 MA 仍上揚)
    # 條件: 昨天在 MA 上，今天跌破 MA，且 MA 向上
    if prev_close >= prev_ma and close < curr_ma and ma_rising:
        return {
            "type": "buy",
            "score": 5,
            "title": "葛蘭碧法則2 (買進)",
            "desc": "假跌破：股價跌破年線，但年線維持上揚趨勢，視為洗盤。",
            "ma200": float(f"{curr_ma:.2f}")
        }

    # 法則 3: 回測支撐 (股價回測 MA 不破且反彈)
    # 條件: 最低價接近 MA (例如 1.5% 內) 但收盤價 > MA，且收紅 K (Close > Open)
    dist_to_ma = (latest['Low'] - curr_ma) / curr_ma
    if 0 < dist_to_ma < 0.015 and close > latest['Open'] and ma_rising:
        return {
            "type": "buy",
            "score": 4,
            "title": "葛蘭碧法則3 (買進)",
            "desc": "回測支撐：股價回測年線不破，且收紅K確認支撐。",
            "ma200": float(f"{curr_ma:.2f}")
        }

    # --- 出場訊號 (賣出) ---
    
    # 法則 6: 假突破 (股價突破 MA，但 MA 下彎)
    if prev_close <= prev_ma and close > curr_ma and ma_falling:
        return {
            "type": "sell",
            "score": -5,
            "title": "葛蘭碧法則6 (賣出)",
            "desc": "假突破：股價突破年線，但年線持續下彎，屬反彈逃命波。",
            "ma200": float(f"{curr_ma:.2f}")
        }

    # 法則 7: 反彈壓力 (股價反彈至 MA 不過且回跌)
    dist_to_ma_high = (curr_ma - latest['High']) / curr_ma
    if 0 < dist_to_ma_high < 0.015 and close < latest['Open'] and ma_falling:
        return {
            "type": "sell",
            "score": -4,
            "title": "葛蘭碧法則7 (賣出)",
            "desc": "反彈遇壓：股價反彈至年線不過，且收黑K確認壓力。",
            "ma200": float(f"{curr_ma:.2f}")
        }
        
    return None

# ==========================================
# 策略 3: 主動式 ETF 籌碼分析
# ==========================================
def strategy_active_etf(ticker, latest_price):
    # 檢查此股票是否在 MOCK_ETF_DB 中
    held_by = []
    total_shares = 0
    total_value = 0
    
    for etf_code, data in MOCK_ETF_DB.items():
        if ticker in data['holdings']:
            h_data = data['holdings'][ticker]
            shares = h_data['shares']
            val = shares * 1000 * latest_price # 估算金額 (shares是張數)
            
            held_by.append({
                "etf_code": etf_code,
                "etf_name": data['name'],
                "shares": shares,
                "pct": h_data['pct'],
                "value": val
            })
            total_shares += shares
            total_value += val
            
    if len(held_by) > 0:
        # 只要有被任何一檔持有就列出 (實際應用可設定門檻，例如至少被2檔持有)
        return {
            "count": len(held_by),
            "total_shares": total_shares,
            "total_value": total_value,
            "details": held_by
        }
    return None

# --- 共用工具函式 ---
def get_financial_details(stock_obj):
    data = {"pe": 999, "growth": None, "rev_yoy": None, "rev_qoq": None, "quarters": []}
    try:
        info = stock_obj.info
        data['pe'] = info.get('trailingPE', 999)
        data['growth'] = info.get('earningsGrowth', None)
        data['rev_yoy'] = info.get('revenueGrowth', None)
        
        q_stmt = stock_obj.quarterly_income_stmt
        if q_stmt is not None and not q_stmt.empty:
            if 'Total Revenue' in q_stmt.index: vals = q_stmt.loc['Total Revenue']
            elif 'Operating Revenue' in q_stmt.index: vals = q_stmt.loc['Operating Revenue']
            else: vals = None
            
            if vals is not None:
                limit = min(4, len(vals))
                for i in range(limit):
                    curr = vals[i]
                    qoq = None
                    if i+1 < len(vals) and vals[i+1] != 0:
                        qoq = (curr - vals[i+1]) / vals[i+1]
                    data['quarters'].append({
                        "date": vals.index[i].strftime('%Y-%m'),
                        "revenue": curr,
                        "qoq": qoq
                    })
    except: pass
    return data

def analyze_stock(stock_info):
    ticker = stock_info['code']
    region = stock_info['region']
    
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="3y") # 還原權值
        if len(df) < 205: return None
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 抓取基本面 (共用)
        fin_data = get_financial_details(stock)
        display_name = get_stock_name(ticker, region, stock)
        
        # 建立基本資料物件
        base_info = {
            "code": ticker,
            "name": display_name,
            "region": region,
            "price": float(f"{latest['Close']:.2f}"),
            "date": latest.name.strftime('%Y-%m-%d'),
            "fundamentals": fin_data
        }
        
        result_pkg = {}
        has_result = False

        # 1. 執行動能策略
        mom_res = strategy_momentum(df, ticker, region, latest, prev, fin_data)
        if mom_res:
            result_pkg['momentum'] = {**base_info, **mom_res}
            has_result = True
            
        # 2. 執行葛蘭碧策略
        gran_res = strategy_granville(df, ticker, region, latest, prev)
        if gran_res:
            result_pkg['granville'] = {**base_info, **gran_res}
            has_result = True
            
        # 3. 執行 ETF 策略
        etf_res = strategy_active_etf(ticker, latest['Close'])
        if etf_res:
            result_pkg['active_etf'] = {**base_info, **etf_res}
            has_result = True
            
        return result_pkg if has_result else None

    except Exception as e:
        return None

def main():
    print("啟動多重策略掃描 (動能 / 葛蘭碧 / 主動ETF)...")
    
    tw_stocks = get_tw_stock_list()
    # us_stocks = get_us_stock_list() # 暫時註解以加速測試，可自行打開
    all_stocks = tw_stocks # + us_stocks
    
    results = {
        "momentum": [],
        "granville_buy": [],
        "granville_sell": [],
        "active_etf": []
    }
    
    print(f"掃描中 ({len(all_stocks)} 檔)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, s) for s in all_stocks]
        for future in as_completed(futures):
            res = future.result()
            if res:
                if 'momentum' in res: results['momentum'].append(res['momentum'])
                if 'granville' in res:
                    if res['granville']['type'] == 'buy': results['granville_buy'].append(res['granville'])
                    else: results['granville_sell'].append(res['granville'])
                if 'active_etf' in res: results['active_etf'].append(res['active_etf'])

    # 排序
    results['momentum'].sort(key=lambda x: -x['score'])
    results['active_etf'].sort(key=lambda x: -x['total_value']) # ETF按持有總金額排序
    
    # 存檔
    market_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 讀取舊檔並更新
    final_history = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                final_history = json.load(f)
        except: pass
        
    new_record = {
        "date": market_date,
        "strategies": results # 新結構
    }
    
    # 簡單去重邏輯
    if final_history and final_history[-1]['date'] == market_date:
        final_history[-1] = new_record
    else:
        final_history.append(new_record)
        
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_history, f, ensure_ascii=False, indent=2)

    print("掃描完成！多策略資料已整合。")

if __name__ == "__main__":
    main()
