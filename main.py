import yfinance as yf
import pandas as pd
import twstock
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os

# --- 設定參數 ---
LOOKBACK_LONG = 500  # 林則行: 兩年新高 (約500交易日)
MA_SHORT = 60        # 季線
VOL_MA = 20          # 成交量均線

def get_tw_stock_list():
    """取得台灣上市櫃股票代號清單"""
    print("正在抓取股票代號清單...")
    # 上市
    twse = twstock.twse
    # 上櫃
    tpex = twstock.tpex
    
    # 這裡我們先篩選常見的股票，避免抓到權證或奇怪的商品
    # 簡單過濾：代號必須是 4 位數
    stocks = []
    for code in twse:
        if len(code) == 4:
            stocks.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4:
            stocks.append(f"{code}.TWO")
            
    print(f"共取得 {len(stocks)} 檔股票代號")
    return stocks

def analyze_stock(ticker):
    """分析單一股票是否符合林則行策略"""
    try:
        stock = yf.Ticker(ticker)
        # 抓取歷史資料 (稍微多抓一點以計算均線)
        df = stock.history(period="2y")
        
        if len(df) < 250: # 上市不滿一年先跳過
            return None

        # 取得最新與前一日資料
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 0. 基本過濾：今日成交量太低(殭屍股)跳過
        if latest['Volume'] < 500000: # 少於500張
            return None

        # --- 林則行策略計算 ---
        
        # 1. 兩年新高判斷 (不含今日)
        # 注意：若資料不足500日，就用現有資料的最大值
        lookback_days = min(len(df)-1, LOOKBACK_LONG)
        window_high = df['Close'][-lookback_days:-1].max()
        is_breaking_high = latest['Close'] > window_high
        
        # 2. 60日均線(季線)趨勢
        ma60 = df['Close'].rolling(window=MA_SHORT).mean()
        curr_ma60 = ma60.iloc[-1]
        prev_ma60 = ma60.iloc[-2]
        is_ma60_up = curr_ma60 > prev_ma60
        is_above_ma60 = latest['Close'] > curr_ma60

        # 3. 成交量爆發
        vol_ma20 = df['Volume'].rolling(window=VOL_MA).mean()
        curr_vol_ma20 = vol_ma20.iloc[-1]
        is_volume_spike = latest['Volume'] > (curr_vol_ma20 * 1.5)

        # --- 評分 (滿分5分) ---
        score = 0
        reasons = []

        if is_breaking_high:
            score += 2
            reasons.append("突破兩年新高")
        
        if is_ma60_up:
            score += 1
            reasons.append("季線向上")
            
        if is_above_ma60:
            score += 1
            reasons.append("站上季線")
            
        if is_volume_spike:
            score += 1
            reasons.append("成交量爆發")

        # 只回傳高分股票 (例如 4分以上) 以節省報告長度
        if score >= 4:
            return {
                "Code": ticker,
                "Price": f"{latest['Close']:.2f}",
                "Score": score,
                "Volume": int(latest['Volume']),
                "Reasons": ", ".join(reasons)
            }
        return None

    except Exception:
        return None

def main():
    start_time = time.time()
    all_stocks = get_tw_stock_list()
    
    # 測試用：為了避免跑太久，你可以先限制只跑前 100 檔
    # all_stocks = all_stocks[:100] 
    
    results = []
    
    print("開始掃描 (這可能需要幾分鐘)...")
    
    # 使用多執行緒加速 (GitHub Actions 通常可以承受 10-20 threads)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, code) for code in all_stocks]
        for future in futures:
            res = future.result()
            if res:
                results.append(res)

    # 排序：分數高 -> 價格高
    results.sort(key=lambda x: (-x['Score'], -float(x['Price'])))

    # --- 產生 Markdown 報告 ---
    report_content = f"# 📈 林則行《大漲的訊號》自動篩選報告\n\n"
    report_content += f"**更新時間**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n\n"
    report_content += f"**篩選標準**: 突破兩年新高(2分)、季線向上(1分)、站上季線(1分)、量增1.5倍(1分)\n\n"
    report_content += f"**總掃描檔數**: {len(all_stocks)} | **符合條件**: {len(results)}\n\n"
    report_content += "---\n\n"
    report_content += "| 代號 | 股價 | 分數 | 觸發條件 | 成交量 |\n"
    report_content += "|---|---|---|---|---|\n"

    for r in results:
        # 將 .TW / .TWO 拿掉顯示比較乾淨
        clean_code = r['Code'].replace('.TW', '').replace('.TWO', '')
        # 產生 Yahoo股市連結
        link = f"[{clean_code}](https://tw.stock.yahoo.com/quote/{clean_code})"
        report_content += f"| {link} | {r['Price']} | **{r['Score']}** | {r['Reasons']} | {r['Volume']:,} |\n"

    # 寫入 README.md (這樣一進 GitHub 首頁就看得到)
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"掃描完成！耗時 {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    main()
