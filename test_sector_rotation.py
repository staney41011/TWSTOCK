import unittest
from datetime import date, timedelta

from sector_rotation import build_sector_rotation_report, pct, snapshot_stock


def make_data(days=115):
    dates = [(date(2026, 1, 1) + timedelta(days=i)).isoformat() for i in range(days)]
    sample = []
    for industry, step in (("半導體業", 0.012), ("航運業", -0.003)):
        for n in range(4):
            bars = []
            for i, day in enumerate(dates):
                bars.append([day, 100 * ((1 + step) ** i), 1000 * (1.5 if i >= days - 5 else 1)])
            sample.append({"code": str(2000 + n if industry == "半導體業" else 2600 + n),
                           "name": industry + str(n), "industry": industry, "bars": bars})
    benchmark = [[day, 1000 * (1.002 ** i)] for i, day in enumerate(dates)]
    return sample, benchmark


class SectorRotationTests(unittest.TestCase):
    def test_returns(self):
        self.assertAlmostEqual(pct(100, 110), 10)
        self.assertIsNone(pct(0, 110))
        self.assertIsNone(pct(100, None))

    def test_empty(self):
        report = build_sector_rotation_report([], [])
        self.assertEqual(report["days"], [])
        self.assertEqual(report["coverage"]["stocks"], 0)

    def test_benchmark_and_sign(self):
        stocks, benchmark = make_data()
        report = build_sector_rotation_report(stocks, benchmark)
        self.assertEqual(report["benchmark"]["source"], "yfinance")
        self.assertEqual(len(report["days"]), 24)
        today = report["days"][-1]
        by_name = {item["name"]: item for item in today["industries"]}
        self.assertGreater(by_name["半導體業"]["strength"], 0)
        self.assertLess(by_name["航運業"]["strength"], 0)
        self.assertGreater(by_name["半導體業"]["volume_ratio"], 1)
        self.assertEqual(by_name["半導體業"]["breadth"], 100)
        self.assertEqual(by_name["航運業"]["breadth"], 0)
        self.assertEqual(by_name["半導體業"]["count"], 4)
        self.assertEqual(len(by_name["半導體業"]["stocks"]), 4)
        self.assertEqual(len(by_name["半導體業"]["relative"]), 5)

    def test_no_index_has_explicit_fallback(self):
        stocks, _ = make_data()
        report = build_sector_rotation_report(stocks, [])
        self.assertEqual(report["benchmark"]["source"], "sample_equal_weight")
        self.assertGreater(report["days"][-1]["industries"][0]["count"], 2)

    def test_no_future_lookahead(self):
        stocks, benchmark = make_data()
        before = build_sector_rotation_report(stocks, benchmark)
        day_to_check = before["days"][3]
        stocks[0]["bars"][-1][1] *= 4
        later = build_sector_rotation_report(stocks, benchmark)
        self.assertEqual(day_to_check, later["days"][3])
        self.assertNotEqual(before["days"][-1], later["days"][-1])

    def test_minimum_coverage(self):
        stocks, bench = make_data()
        few = [s for s in stocks if s["industry"] == "半導體業"][:2]
        report = build_sector_rotation_report(few, bench)
        self.assertEqual(report["days"], [])

    def test_invalid_stock_skipped(self):
        stocks, benchmark = make_data()
        stocks.append({"code": "0000", "name": "錯誤", "industry": "半導體業",
                       "bars": [["2026-01-02", -100, 0]] * 110})
        report = build_sector_rotation_report(stocks, benchmark)
        self.assertEqual(report["coverage"]["stocks"], 8)

    def test_snapshot_stock(self):
        # Avoid requiring pandas for core tests; a small dataframe-compatible fixture.
        class FakeFrame:
            def __init__(self):
                self.rows = [
                    (date(2026, 1, 1) + timedelta(days=i), {"Close": 100 + i, "Volume": 1200})
                    for i in range(80)
                ]
            def __len__(self): return len(self.rows)
            def tail(self, n): return self
            def iterrows(self):
                for timestamp, row in self.rows:
                    yield timestamp, row
        obj = snapshot_stock("2330.TW", "台積電", "半導體業", FakeFrame())
        self.assertEqual(obj["code"], "2330")
        self.assertEqual(len(obj["bars"]), 80)


if __name__ == "__main__":
    unittest.main()
