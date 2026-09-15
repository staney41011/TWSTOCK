from pathlib import Path
import re


def replace_once(text, old, new, label):
    if old not in text:
        raise RuntimeError(f"找不到待替換區塊: {label}")
    return text.replace(old, new, 1)


def regex_once(text, pattern, replacement, label):
    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"找不到待替換區塊: {label} (count={count})")
    return new_text


def patch_index():
    path = Path("index.html")
    text = path.read_text(encoding="utf-8")

    text = replace_once(text, "xl:grid-cols-9", "xl:grid-cols-10", "summary grid")
    momentum_anchor = '''            <div class="surface rounded-lg border p-4">\n                <div class="text-xs text-stone-500">動能爆發</div>'''
    consensus_card = '''            <div class="surface rounded-lg border p-4 border-l-4 border-l-cyan-400">\n                <div class="text-xs text-stone-500">策略共振</div>\n                <div class="mt-1 text-2xl font-semibold">{{ strategyCount('consensus') }}</div>\n            </div>\n''' + momentum_anchor
    text = replace_once(text, momentum_anchor, consensus_card, "consensus summary card")

    text = replace_once(text, "currentStrategy: 'momentum',", "currentStrategy: 'consensus',", "default tab")
    text = replace_once(text, "            history: [],\n", "            history: [],\n            recordCache: {},\n            currentRecordData: {},\n", "lazy state")
    text = replace_once(
        text,
        "            tabs: [\n                { key: 'momentum', label: '動能爆發' },",
        "            tabs: [\n                { key: 'consensus', label: '策略共振' },\n                { key: 'momentum', label: '動能爆發' },",
        "consensus tab",
    )

    text = replace_once(
        text,
        "        currentRecord() { return this.reversedHistory[this.selectedDateIndex] || {}; },",
        "        currentRecord() {\n            const meta = this.reversedHistory[this.selectedDateIndex] || {};\n            return this.currentRecordData?.date === meta.date ? this.currentRecordData : meta;\n        },",
        "current record",
    )
    text = replace_once(
        text,
        "        currentItems() {\n            if (['holy_grail', 'key_branches', 'druckenmiller'].includes(this.currentStrategy)) return [];\n            const rows = this.strategyData[this.currentStrategy];\n            return Array.isArray(rows) ? rows : [];\n        },",
        "        currentItems() {\n            if (this.currentStrategy === 'consensus') return this.consensusItems;\n            if (['holy_grail', 'key_branches', 'druckenmiller'].includes(this.currentStrategy)) return [];\n            const rows = this.strategyData[this.currentStrategy];\n            return Array.isArray(rows) ? rows : [];\n        },",
        "current items",
    )

    consensus_computed = '''        consensusItems() {\n            const buckets = new Map();\n            const addRows = (rows, source, weight, predicate = null) => {\n                if (!Array.isArray(rows)) return;\n                rows.forEach(item => {\n                    if (predicate && !predicate(item)) return;\n                    const rawCode = item?.code || item?.stock_code || item?.stockCode || '';\n                    const code = this.baseCode(rawCode);\n                    if (!code) return;\n                    const priceValue = Number(item?.price ?? item?.close);\n                    const existing = buckets.get(code) || {\n                        code: rawCode || code,\n                        name: item?.name || code,\n                        price: Number.isFinite(priceValue) ? priceValue : null,\n                        score: 0,\n                        hit_count: 0,\n                        sources: [],\n                        reasons: [],\n                        fundamentals: item?.fundamentals || {},\n                    };\n                    if (!existing.sources.includes(source)) {\n                        existing.sources.push(source);\n                        existing.score += weight;\n                        existing.hit_count = existing.sources.length;\n                        existing.reasons.push(`${source} 出現訊號`);\n                    }\n                    if ((!existing.name || existing.name === code) && item?.name) existing.name = item.name;\n                    if (!Number.isFinite(Number(existing.price)) && Number.isFinite(priceValue)) existing.price = priceValue;\n                    if ((!existing.fundamentals || !Object.keys(existing.fundamentals).length) && item?.fundamentals) existing.fundamentals = item.fundamentals;\n                    buckets.set(code, existing);\n                });\n            };\n\n            addRows(this.strategyData.momentum, '動能爆發', 3);\n            addRows(this.strategyData.doji_rise, '十字星', 2);\n            addRows(this.strategyData.day_trading, '隔日沖', 2);\n            addRows(this.strategyData.macd_turn_red, 'MACD翻紅', 2);\n            addRows(this.strategyData.cbas, '可轉債 CBAS', 3);\n            addRows(this.strategyData.active_etf, '主動 ETF', 3, item => Number(item?.buy_count || 0) > Number(item?.sell_count || 0));\n            addRows(this.holyGrailCandidates.breakout, '聖杯突破', 4);\n            addRows(this.holyGrailCandidates.pullback, '聖杯回檔', 4);\n            addRows(this.druckCandidates, 'Druck 雷達', 4);\n            addRows(this.keyBranchItems, '關鍵分點', 3, item => item?.bias === '偏多');\n\n            return [...buckets.values()]\n                .filter(item => item.hit_count >= 2)\n                .sort((a, b) => b.hit_count - a.hit_count || b.score - a.score || String(a.code).localeCompare(String(b.code)));\n        },\n'''
    text = replace_once(text, "        holyGrailReport() {\n", consensus_computed + "        holyGrailReport() {\n", "consensus computed")

    consensus_template = '''                <template v-if="currentStrategy === 'consensus'">\n                    <div class="flex items-start justify-between gap-4">\n                        <div>\n                            <h2 class="text-lg font-semibold text-stone-50">{{ item.name }}</h2>\n                            <p class="text-sm text-stone-400">{{ baseCode(item.code) }} · {{ formatPrice(item.price) }}</p>\n                        </div>\n                        <div class="text-right">\n                            <div class="rounded-md bg-cyan-300 px-2 py-1 text-sm font-bold text-zinc-950">{{ item.hit_count }} 策略</div>\n                            <div class="mt-2 text-xs text-cyan-200">共振分數 {{ item.score }}</div>\n                        </div>\n                    </div>\n                    <div class="mt-4 flex flex-wrap gap-2">\n                        <span v-for="source in item.sources" :key="source" class="rounded-md bg-cyan-400/10 px-2 py-1 text-xs font-semibold text-cyan-100">{{ source }}</span>\n                    </div>\n                    <p class="mt-4 text-xs leading-5 text-stone-500">同一檔股票同時被至少 2 個獨立策略偵測，策略數優先、加權分數次之。</p>\n                </template>\n\n                <template v-else-if="currentStrategy === 'active_etf'">'''
    text = replace_once(text, "                <template v-if=\"currentStrategy === 'active_etf'\">", consensus_template, "consensus card template")

    load_methods = '''        async loadData(options = {}) {\n            const silent = options?.silent === true;\n            const previousDate = this.currentRecord.date || this.reversedHistory[this.selectedDateIndex]?.date;\n            if (!silent) this.loading = true;\n            this.errorMsg = '';\n            try {\n                const stamp = Date.now();\n                const manifestResponse = await fetch(`./data/manifest.json?t=${stamp}`, { cache: 'no-store' });\n                if (manifestResponse.ok) {\n                    const manifest = await manifestResponse.json();\n                    if (!Array.isArray(manifest)) throw new Error('manifest.json 格式不是陣列');\n                    this.history = manifest;\n                } else {\n                    const fallback = await fetch(`./data.json?t=${stamp}`, { cache: 'no-store' });\n                    if (!fallback.ok) throw new Error(`HTTP ${fallback.status}`);\n                    const legacy = await fallback.json();\n                    if (!Array.isArray(legacy)) throw new Error('data.json 格式不是陣列');\n                    this.history = legacy.map(record => ({\n                        date: record.date,\n                        market_breadth: record.market_breadth,\n                    }));\n                    this.recordCache = Object.fromEntries(legacy.filter(record => record?.date).map(record => [record.date, record]));\n                }\n\n                const reversed = [...this.history].reverse();\n                const previousIndex = previousDate ? reversed.findIndex(record => record.date === previousDate) : -1;\n                this.selectedDateIndex = previousIndex >= 0 ? previousIndex : 0;\n                await this.loadSelectedDate({ force: true });\n                this.lastLoadedAt = new Date().toLocaleString('zh-TW', { hour12: false });\n            } catch (error) {\n                if (!silent) {\n                    this.history = [];\n                    this.currentRecordData = {};\n                }\n                this.errorMsg = error.message;\n            } finally {\n                if (!silent) this.loading = false;\n            }\n        },\n        async loadSelectedDate(options = {}) {\n            const force = options?.force === true;\n            const meta = this.reversedHistory[this.selectedDateIndex];\n            if (!meta?.date) {\n                this.currentRecordData = {};\n                return;\n            }\n            if (!force && this.recordCache[meta.date]) {\n                this.currentRecordData = this.recordCache[meta.date];\n                return;\n            }\n            this.currentRecordData = meta;\n            const response = await fetch(`./data/${meta.date}.json?t=${Date.now()}`, { cache: 'no-store' });\n            if (!response.ok) {\n                if (this.recordCache[meta.date]) {\n                    this.currentRecordData = this.recordCache[meta.date];\n                    return;\n                }\n                throw new Error(`單日資料 ${meta.date} 讀取失敗：HTTP ${response.status}`);\n            }\n            const record = await response.json();\n            this.recordCache[meta.date] = record;\n            this.currentRecordData = record;\n        },\n        strategyCount(key) {'''
    text = regex_once(
        text,
        r"        async loadData\(options = \{\}\) \{.*?\n        \},\n        strategyCount\(key\) \{",
        load_methods,
        "lazy load methods",
    )
    text = replace_once(
        text,
        "        strategyCount(key) {\n            if (key === 'holy_grail') return this.holyGrailCandidateTotal();",
        "        strategyCount(key) {\n            if (key === 'consensus') return this.consensusItems.length;\n            if (key === 'holy_grail') return this.holyGrailCandidateTotal();",
        "consensus count",
    )

    watch_block = '''    watch: {\n        selectedDateIndex(newIndex, oldIndex) {\n            if (newIndex !== oldIndex && !this.loading) {\n                this.loadSelectedDate().catch(error => { this.errorMsg = error.message; });\n            }\n        },\n    },\n    methods: {'''
    text = replace_once(text, "    methods: {", watch_block, "date watcher")

    path.write_text(text, encoding="utf-8")


def patch_main():
    path = Path("main.py")
    text = path.read_text(encoding="utf-8")
    text = replace_once(text, "from key_branches import empty_key_branch_report, generate_key_branch_report\n", "from key_branches import empty_key_branch_report, generate_key_branch_report\nfrom data_index import rebuild_data_indexes\n", "main import")
    text = regex_once(
        text,
        r"    all_files = sorted\(glob\.glob\(os\.path\.join\(DATA_DIR, \"\\\*\.json\"\)\)\).*?    with open\(DATA_FILE, 'w', encoding='utf-8'\) as f:\n        json\.dump\(clean_for_json\(final_history\), f, ensure_ascii=False, indent=2\)\n",
        "    rebuild_data_indexes(DATA_DIR, DATA_FILE)\n",
        "main history rebuild",
    )
    path.write_text(text, encoding="utf-8")


def patch_rerun():
    path = Path("rerun_holy_grail.py")
    path.write_text('''import argparse\nimport json\nfrom pathlib import Path\n\nfrom data_index import latest_data_date, rebuild_data_indexes\nfrom holy_grail import generate_holy_grail_report_from_yfinance\nfrom main import DATA_DIR, DATA_FILE, clean_for_json\n\n\ndef load_json(path, default):\n    if not path.exists():\n        return default\n    with path.open("r", encoding="utf-8") as file:\n        return json.load(file)\n\n\ndef write_json(path, data):\n    path.parent.mkdir(parents=True, exist_ok=True)\n    with path.open("w", encoding="utf-8") as file:\n        json.dump(clean_for_json(data), file, ensure_ascii=False, indent=2)\n\n\ndef main():\n    parser = argparse.ArgumentParser(description="重新產生台股聖杯雷達與美股產業對應資料")\n    parser.add_argument("--date", help="指定資料日期，格式 YYYY-MM-DD。未指定時使用最新單日資料。")\n    parser.add_argument("--max-per-industry", type=int, default=8, help="每個細分類最多抓取幾檔台股。")\n    args = parser.parse_args()\n\n    target_date = args.date or latest_data_date(DATA_DIR, DATA_FILE)\n    if not target_date:\n        raise SystemExit("找不到可重跑日期，請指定 --date 或先執行 main.py。")\n\n    print(f"重新產生台股聖杯雷達：{target_date}")\n    report = clean_for_json(generate_holy_grail_report_from_yfinance(\n        target_date=target_date,\n        max_per_industry=args.max_per_industry,\n    ))\n\n    daily_path = Path(DATA_DIR) / f"{target_date}.json"\n    daily = load_json(daily_path, {"date": target_date, "market_breadth": None, "strategies": {}})\n    daily.setdefault("strategies", {})["holy_grail"] = report\n    write_json(daily_path, daily)\n    rebuild_data_indexes(DATA_DIR, DATA_FILE)\n\n    counts = {key: len(value) for key, value in report.get("candidates", {}).items()}\n    print(f"完成：market={report.get('market', {}).get('state')} candidates={counts} usMatches={len(report.get('usTaiwanMatches', []))}")\n\n\nif __name__ == "__main__":\n    main()\n''', encoding="utf-8")


def patch_backfill():
    path = Path("backfill.py")
    text = path.read_text(encoding="utf-8")
    text = replace_once(text, "from datetime import datetime, timedelta\n", "from datetime import datetime, timedelta\nfrom data_index import rebuild_data_indexes\n", "backfill import")
    text = regex_once(
        text,
        r"    final_history = \[\].*?    with open\(OUTPUT_FILE, 'w', encoding='utf-8'\) as f: json\.dump\(clean_for_json\(final_history\), f, ensure_ascii=False, indent=2\)\n",
        "    rebuild_data_indexes(DATA_DIR, OUTPUT_FILE)\n",
        "backfill history rebuild",
    )
    path.write_text(text, encoding="utf-8")


def patch_workflow():
    path = Path(".github/workflows/daily_scan.yml")
    text = path.read_text(encoding="utf-8")
    text = replace_once(text, "    - cron: '0 * * * 1-5'", "    - cron: '0 0-9 * * 1-5'", "trading-hour schedule")
    text = replace_once(text, "permissions:\n  contents: write\n", "permissions:\n  contents: write\n\nconcurrency:\n  group: twstock-daily-scan\n  cancel-in-progress: false\n", "workflow concurrency")
    text = replace_once(text, "uses: actions/checkout@v3", "uses: actions/checkout@v4", "checkout v4")
    text = replace_once(text, "uses: actions/setup-python@v4", "uses: actions/setup-python@v5", "setup-python v5")
    text = replace_once(text, "        python-version: '3.10'\n", "        python-version: '3.10'\n        cache: 'pip'\n", "pip cache")
    path.write_text(text, encoding="utf-8")


def main():
    patch_index()
    patch_main()
    patch_rerun()
    patch_backfill()
    patch_workflow()
    print("TWSTOCK upgrade patch applied")


if __name__ == "__main__":
    main()
