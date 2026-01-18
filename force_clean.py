import json
import os
import math
import glob

# 設定
TARGET_FILE = "data.json"
DATA_DIR = "data"

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
    print("🚑 啟動強制清洗程序 (移除所有 NaN)...")

    # 1. 清洗 data.json
    if os.path.exists(TARGET_FILE):
        try:
            with open(TARGET_FILE, 'r', encoding='utf-8') as f:
                # Python json 模組可以讀取 NaN，不會報錯
                data = json.load(f)
            
            cleaned_data = clean_nan(data)
            
            with open(TARGET_FILE, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
            print(f"✅ {TARGET_FILE} 清洗完成！")
        except Exception as e:
            print(f"❌ {TARGET_FILE} 讀取失敗: {e}")

    # 2. 清洗 data/ 資料夾內所有檔案
    if os.path.exists(DATA_DIR):
        files = glob.glob(os.path.join(DATA_DIR, "*.json"))
        print(f"📂 正在檢查 {len(files)} 個分日檔案...")
        
        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                cleaned_data = clean_nan(data)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
                # print(f"   -> 已清洗: {file_path}") 
            except Exception as e:
                print(f"❌ {file_path} 清洗失敗: {e}")
                
    print("\n🎉 所有檔案已清洗完畢！網頁應該可以正常顯示了。")
    print("註：1月12日顯示無資料是正常的，因為備份檔當天就是空的。")

if __name__ == "__main__":
    main()
