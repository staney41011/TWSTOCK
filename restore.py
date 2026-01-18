import json
import os
import glob
from datetime import datetime

# 設定
BACKUP_FILE = "old_backup.json" # 您剛剛救回來的舊檔案
DATA_DIR = "data"               # 新的資料夾
OUTPUT_FILE = "data.json"       # 最後要生成的總檔

def restore_and_migrate():
    print("🚀 開始執行資料救援與搬家...")

    # 1. 確保資料夾存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # 2. 讀取舊備份資料
    if os.path.exists(BACKUP_FILE):
        try:
            with open(BACKUP_FILE, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
            
            print(f"📄 讀取到 {len(old_data)} 筆舊資料，開始轉換...")

            for record in old_data:
                date_str = record.get('date')
                
                # 跳過無效日期
                if not date_str: continue

                # 轉換舊格式 (只有 buy) -> 新格式 (strategies.momentum)
                new_record = {
                    "date": date_str,
                    "market_breadth": record.get("market_breadth", 0),
                    "strategies": {
                        "momentum": record.get("buy", []), # 舊的 buy 對應動能策略
                        "granville_buy": [],
                        "granville_sell": [],
                        "day_trading": [],
                        "doji_rise": [],
                        "active_etf": [],
                        "low_volatility": []
                    }
                }
                
                # 檢查這筆資料是否已經存在 data/ 中 (避免覆蓋今天剛跑的正確資料)
                target_path = os.path.join(DATA_DIR, f"{date_str}.json")
                if not os.path.exists(target_path):
                    with open(target_path, 'w', encoding='utf-8') as f:
                        json.dump(new_record, f, ensure_ascii=False, indent=2)
                    print(f"✅ 已還原: {date_str}")
                else:
                    print(f"ℹ️ 跳過 (已存在): {date_str}")

        except Exception as e:
            print(f"❌ 讀取備份檔失敗: {e}")
    else:
        print(f"⚠️ 找不到 {BACKUP_FILE}，請確認您已建立此檔案並貼上舊資料。")

    # 3. 清洗錯誤檔案 (週末 & 未來 & 2026)
    print("\n🧹 開始清洗異常檔案...")
    all_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    today = datetime.now().strftime('%Y-%m-%d')
    
    for file_path in all_files:
        filename = os.path.basename(file_path)
        date_str = filename.replace(".json", "")
        
        try:
            # 檢查日期格式
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            
            # 條件A: 刪除未來日期 (含 2026)
            if date_str > today:
                print(f"🗑️ 刪除未來/錯誤日期: {filename}")
                os.remove(file_path)
                continue
                
            # 條件B: 刪除週末 (週六=5, 週日=6)
            # 注意：台股有時有補班日開盤，但通常週末無盤。若您確定是誤判則刪除。
            if dt.weekday() >= 5:
                print(f"🗑️ 刪除週末檔案: {filename}")
                os.remove(file_path)
                continue
                
        except:
            print(f"⚠️ 略過格式錯誤檔案: {filename}")

    # 4. 重新彙整 data.json
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
