# TWSTOCK

台股策略掃描與研究儀表板。後端以 Python 定期掃描台股與相關資料來源，前端以靜態 HTML / Vue 顯示每日策略結果。

## 頁面

- `index.html`：原本的台股策略雷達，包含動能、十字星、隔日沖、MACD、CBAS、主動 ETF、Druckenmiller、台股聖杯與關鍵分點。
- `consensus.html`：策略共振雷達。把多個獨立策略交叉比對，優先列出同時被 2 個以上正向策略偵測的股票，並另外標示過熱、偏賣與分點偏空等風險訊號。

GitHub Pages 啟用後，可直接使用：

- `https://staney41011.github.io/TWSTOCK/`
- `https://staney41011.github.io/TWSTOCK/consensus.html`

## 資料架構

每日完整資料保留在：

```text
data/YYYY-MM-DD.json
```

`data_index.py` 會另外建立：

```text
data/manifest.json
```

這是一份輕量日期索引，只保存日期、市場寬度與各策略筆數。新的頁面優先讀取 manifest，再只載入使用者選定日期的完整 JSON。

目前舊版 `index.html` 仍依賴 `data.json`，因此 `data.json` 暫時保留完整歷史資料，避免既有首頁的歷史日期消失；產生時會使用 compact JSON 減少不必要的空白。等原首頁也完成按日懶載入後，再安全地縮小或移除 legacy `data.json`。

## 策略共振權重

目前共振頁的正向加權如下：

| 訊號 | 分數 |
| --- | ---: |
| 台股聖杯：突破 / 回檔 | 4 |
| Druckenmiller 雷達 | 4 |
| 動能爆發 | 3 |
| 可轉債 CBAS | 3 |
| 主動 ETF 淨買進 | 3 |
| 關鍵分點偏多 | 3 |
| MACD 翻紅 | 2 |
| 十字星 | 2 |
| 隔日沖 | 2 |

主動 ETF 偏賣、台股聖杯過熱 / 出場、關鍵分點偏空只列為風險標記，不計入正向共振分數。

## 自動更新

`.github/workflows/daily_scan.yml` 於台灣時間週一到週五 08:00–19:00 每小時執行一次，並支援手動 Backfill 與只重跑台股聖杯。

每次策略掃描後會：

1. 更新當日 `data/YYYY-MM-DD.json`。
2. 重建 `data/manifest.json`。
3. 重建相容用的完整 `data.json`（compact JSON）。
4. 執行 `test_data_index.py`，確認日期索引、策略筆數與相容資料邏輯正常。

## 本機使用

```bash
pip install -r requirements.txt
python main.py
python -m unittest test_data_index.py
```

本工具僅供研究與教育用途，不構成投資建議。
