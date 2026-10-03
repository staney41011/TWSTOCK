import json
import tempfile
import unittest
from pathlib import Path

from data_index import latest_data_date, rebuild_data_indexes


class DataIndexTests(unittest.TestCase):
    def _write_record(self, directory, date, strategies=None, breadth=12.3):
        record = {
            "date": date,
            "market_breadth": breadth,
            "strategies": strategies or {},
        }
        path = Path(directory) / f"{date}.json"
        path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        return record

    def test_rebuild_creates_manifest_and_bounds_legacy_history(self):
        with tempfile.TemporaryDirectory() as temp:
            data_dir = Path(temp) / "data"
            data_dir.mkdir()
            legacy = Path(temp) / "data.json"

            self._write_record(data_dir, "2026-09-11", {"momentum": [{"code": "1111.TW"}]})
            self._write_record(data_dir, "2026-09-14", {"holy_grail": {"candidates": {"breakout": [{"code": "2222.TW"}], "pullback": []}}})
            self._write_record(data_dir, "2026-09-15", {"key_branches": {"items": [{"code": "3333"}, {"code": "4444"}]}})

            manifest = rebuild_data_indexes(data_dir, legacy, legacy_limit=2)

            self.assertEqual([item["date"] for item in manifest], ["2026-09-11", "2026-09-14", "2026-09-15"])
            self.assertEqual(manifest[0]["counts"]["momentum"], 1)
            self.assertEqual(manifest[1]["counts"]["holy_grail"], 1)
            self.assertEqual(manifest[2]["counts"]["key_branches"], 2)

            legacy_data = json.loads(legacy.read_text(encoding="utf-8"))
            self.assertEqual([item["date"] for item in legacy_data], ["2026-09-14", "2026-09-15"])
            self.assertEqual(latest_data_date(data_dir, legacy), "2026-09-15")

    def test_manifest_file_is_not_mistaken_for_daily_record(self):
        with tempfile.TemporaryDirectory() as temp:
            data_dir = Path(temp) / "data"
            data_dir.mkdir()
            legacy = Path(temp) / "data.json"
            self._write_record(data_dir, "2026-09-15")
            (data_dir / "manifest.json").write_text('[{"date":"1900-01-01"}]', encoding="utf-8")

            manifest = rebuild_data_indexes(data_dir, legacy)
            self.assertEqual(len(manifest), 1)
            self.assertEqual(manifest[0]["date"], "2026-09-15")


if __name__ == "__main__":
    unittest.main()
