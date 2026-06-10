import argparse
import json
from pathlib import Path

from holy_grail import generate_holy_grail_report_from_yfinance
from main import DATA_DIR, DATA_FILE, clean_for_json


def load_json(path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(clean_for_json(data), file, ensure_ascii=False, indent=2)


def find_or_create_record(history, target_date):
    for record in history:
        if record.get("date") == target_date:
            return record
    record = {"date": target_date, "market_breadth": None, "strategies": {}}
    history.append(record)
    history.sort(key=lambda item: item.get("date", ""))
    return record


def main():
    parser = argparse.ArgumentParser(description="重新產生台股聖杯雷達與美股產業對應資料")
    parser.add_argument("--date", help="指定資料日期，格式 YYYY-MM-DD。未指定時使用 data.json 最新日期。")
    parser.add_argument("--max-per-industry", type=int, default=8, help="每個細分類最多抓取幾檔台股。")
    args = parser.parse_args()

    history_path = Path(DATA_FILE)
    history = load_json(history_path, [])
    if not history and not args.date:
        raise SystemExit("data.json 沒有資料，請指定 --date 或先執行 main.py。")

    target_date = args.date or history[-1]["date"]
    print(f"重新產生台股聖杯雷達：{target_date}")
    report = clean_for_json(generate_holy_grail_report_from_yfinance(
        target_date=target_date,
        max_per_industry=args.max_per_industry,
    ))

    record = find_or_create_record(history, target_date)
    record.setdefault("strategies", {})["holy_grail"] = report
    write_json(history_path, history)

    daily_path = Path(DATA_DIR) / f"{target_date}.json"
    daily = load_json(daily_path, {"date": target_date, "market_breadth": record.get("market_breadth"), "strategies": {}})
    daily.setdefault("strategies", {})["holy_grail"] = report
    write_json(daily_path, daily)

    counts = {key: len(value) for key, value in report.get("candidates", {}).items()}
    print(f"完成：market={report.get('market', {}).get('state')} candidates={counts} usMatches={len(report.get('usTaiwanMatches', []))}")


if __name__ == "__main__":
    main()
