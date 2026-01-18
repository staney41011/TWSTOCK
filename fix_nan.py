import json
import os
import math
import glob

# 設定
TARGET_FILE = "data.json"
DATA_DIR = "data"

def clean_nan(obj):
    """
    只負責把 NaN 和 Infinity 轉成 null，不動其他資料
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
    print("🚑 啟動 NaN 修復程序 (保留 2026 日期)...")

    if not os.path.exists(TARGET_FILE):
        print(f"❌ 找不到 {TARGET_FILE}")
        return

    # 1. 讀取壞掉的檔案
    with open(TARGET_FILE, 'r', encoding='utf-8') as f:
        # Python 的 json 模組可以容忍 NaN，所以這裡讀取沒問題
        raw_data = json.load(f)
    
    print(f"📄 讀取成功，共 {len(raw_data)} 筆資料")

    # 2. 清洗 NaN
    cleaned_data = clean_nan(raw_data)

    # 3. 覆蓋回 data.json (變成標準 JSON)
    with open(TARGET_FILE, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
    print(f"✅ {TARGET_FILE} 已修復 (NaN -> null)")

    # 4. 同步更新 data/ 資料夾
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for record in cleaned_data:
        date_str = record.get('date')
        if date_str:
            file_path = os.path.join(DATA_DIR, f"{date_str}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            print(f"   -> 同步更新: {file_path}")

if __name__ == "__main__":
    main()
