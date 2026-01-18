import json
import os
import math
import glob

# 設定檔案路徑
BACKUP_FILE = "old_backup.json"
DATA_DIR = "data"
OUTPUT_FILE = "data.json"

def clean_nan(obj):
    """
    遞迴將 NaN / Infinity 轉為 None (JSON null)
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    return obj

def main():
    print("🚀 啟動全能修復程序 (修復NaN + 還原舊資料)...")

    # 1. 確保資料夾存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # 2. 讀取並修復 old_backup.json
    if os.path.exists(BACKUP_FILE):
        try:
            with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
                raw_backup = json.load(f)
            
            # 清洗 NaN
            clean_backup = clean_nan(raw_backup)
            print(f"📄 讀取備份檔成功，共 {len(clean_backup)} 筆資料。")

            # 開始還原
            for record in clean_backup:
                date_str = record.get('date')
                if not date_str: continue

                # 格式轉換邏輯
                # 如果是舊格式 (有 buy 但沒有 strategies)，幫它搬家
                strategies = record.get("strategies", {})
                
                if "buy" in record and not strategies:
                    # 舊版 buy 對應到新版 momentum
                    strategies = {
                        "momentum": record.get("buy", []), 
                        "granville_buy": [], "granville_sell": [],
                        "day_trading": [], "doji_rise": [],
                        "active_etf": [], "low_volatility": []
                    }
                elif strategies:
                    # 確保新版欄位齊全
                    keys = ["momentum", "granville_buy", "granville_sell", "day_trading", "doji_rise", "active_etf", "low_volatility"]
                    for k in keys:
                        if k not in strategies: strategies[k] = []

                new_record = {
                    "date": date_str,
                    "market_breadth": record.get("market_breadth", 0),
                    "strategies": strategies
                }

                # 寫入單日檔案 (強制覆蓋，確保資料是最新的)
                file_path = os.path.join(DATA_DIR, f"{date_str}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(new_record, f, ensure_ascii=False, indent=2)
                
                count = len(strategies.get('momentum', []))
                print(f"   -> 已還原: {date_str} (含 {count} 筆動能股)")

        except Exception as e:
            print(f"❌ 備份還原失敗: {e}")
    else:
        print(f"⚠️ 找不到 {BACKUP_FILE}，跳過還原步驟。")

    # 3. 重新彙整 data.json
    print("\n📦 正在重新打包 data.json (給網頁讀取)...")
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    final_history = []
    
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 再次清洗以防萬一
                data = clean_nan(data)
                final_history.append(data)
        except: pass
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_history, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 修復完成！網頁資料檔已更新 (共 {len(final_history)} 天)。")
    print("請重新整理網頁，應該就能看到資料了！")

if __name__ == "__main__":
    main()
