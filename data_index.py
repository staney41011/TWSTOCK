import json
import re
from pathlib import Path

DATE_FILE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DEFAULT_LEGACY_HISTORY_LIMIT = 20


def _strategy_count(key, value):
    if isinstance(value, list):
        return len(value)
    if not isinstance(value, dict):
        return 0
    if key == "holy_grail":
        candidates = value.get("candidates") or {}
        return sum(len(rows) for rows in candidates.values() if isinstance(rows, list))
    if key == "druckenmiller":
        return len(value.get("candidates") or [])
    if key == "key_branches":
        return len(value.get("items") or [])
    return 0


def _load_daily_records(data_dir):
    directory = Path(data_dir)
    records = []
    for path in sorted(directory.glob("*.json")):
        if not DATE_FILE_RE.match(path.stem):
            continue
        try:
            with path.open("r", encoding="utf-8") as file:
                record = json.load(file)
            if record.get("date") != path.stem:
                continue
            records.append(record)
        except (OSError, ValueError, TypeError):
            continue
    records.sort(key=lambda item: item.get("date", ""))
    return records


def rebuild_data_indexes(data_dir="data", legacy_file="data.json", legacy_limit=DEFAULT_LEGACY_HISTORY_LIMIT):
    """Rebuild the tiny date manifest and a bounded legacy data.json fallback.

    The website should use data/manifest.json + data/YYYY-MM-DD.json.  data.json is
    intentionally kept as a short compatibility window for older clients/tools.
    """
    directory = Path(data_dir)
    directory.mkdir(parents=True, exist_ok=True)
    records = _load_daily_records(directory)

    manifest = []
    for record in records:
        strategies = record.get("strategies") or {}
        manifest.append({
            "date": record.get("date"),
            "market_breadth": record.get("market_breadth"),
            "counts": {
                key: _strategy_count(key, value)
                for key, value in strategies.items()
            },
        })

    manifest_path = directory / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, ensure_ascii=False, separators=(",", ":"))

    legacy_records = records[-max(1, int(legacy_limit)):] if records else []
    with Path(legacy_file).open("w", encoding="utf-8") as file:
        json.dump(legacy_records, file, ensure_ascii=False, separators=(",", ":"))

    return manifest


def latest_data_date(data_dir="data", legacy_file="data.json"):
    manifest_path = Path(data_dir) / "manifest.json"
    try:
        if manifest_path.exists():
            with manifest_path.open("r", encoding="utf-8") as file:
                manifest = json.load(file)
            if isinstance(manifest, list) and manifest:
                return manifest[-1].get("date")
    except (OSError, ValueError, TypeError):
        pass

    records = _load_daily_records(data_dir)
    if records:
        return records[-1].get("date")

    try:
        with Path(legacy_file).open("r", encoding="utf-8") as file:
            history = json.load(file)
        if isinstance(history, list) and history:
            return history[-1].get("date")
    except (OSError, ValueError, TypeError):
        pass
    return None
