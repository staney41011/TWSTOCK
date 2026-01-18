import json
import os
import glob
from datetime import datetime

# 設定
BACKUP_FILE = "old_backup.json" # 您的舊資料備份
DATA_DIR = "data"               # 目標資料夾
OUTPUT_FILE = "data.json"       # 最後彙整的總檔

def restore_and_migrate():
    print("🚀 啟動強制救援模式 (以備份檔為主)...")

    # 1. 確保資料夾存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # 2. 讀取舊備份資料
    if os.path.exists(BACKUP_FILE):
        try:
            with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
            
            print(f"📄 讀取到 {len(old_data)} 筆備份資料，開始強制覆蓋...")

            for record in old_data:
                date_str = record.get('date')
                if not date_str: continue

                # 判斷資料格式 (是舊版 buy 還是新版 strategies)
                strategies = record.get("strategies", {})
                
                # 如果是舊版格式 (有 buy 欄位)，進行轉換
                if "buy" in record and not strategies:
                    strategies = {
                        "momentum": record.get("buy", []),
                        "granville_buy": [],
                        "granville_sell": [],
                        "day_trading": [],
                        "doji_rise": [],
                        "active_etf": [],
                        "low_volatility": []
                    }
                # 如果本來就是新版格式但缺少某些 key，補齊它
                elif strategies:
                    default_keys = ["momentum", "granville_buy", "granville_sell", "day_trading", "doji_rise", "active_etf", "low_volatility"]
                    for k in default_keys:
                        if k not in strategies:
                            strategies[k] = []

                # 建立標準化資料結構
                new_record = {
                    "date": date_str,
                    "market_breadth": record.get("market_breadth", 0),
                    "strategies": strategies
                }
                
                # 【關鍵修改】不檢查檔案是否存在，直接強制寫入！
                target_path = os.path.join(DATA_DIR, f"{date_str}.json")
                with open(target_path, 'w', encoding='utf-8') as f:
                    json.dump(new_record, f, ensure_ascii=False, indent=2)
                print(f"✅ 強制還原: {date_str} (包含 {len(strategies.get('momentum', []))} 筆動能股)")

        except Exception as e:
            print(f"❌ 讀取備份檔失敗: {e}")
    else:
        print(f"⚠️ 找不到 {BACKUP_FILE}，無法執行還原。")

    # 3. 重新彙整 data.json
    print("\n📦 正在重新打包 data.json...")
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.json")))
    final_history = []
    
    for file_path in all_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                final_history.append(json.load(f))
        except: pass
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_history, f, ensure_ascii=False, indent=2)
        
    print(f"🎉 救援完成！目前共有 {len(final_history)} 天的資料。")

if __name__ == "__main__":
    restore_and_migrate()
