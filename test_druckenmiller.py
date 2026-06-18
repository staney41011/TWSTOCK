import unittest

from druckenmiller import generate_druckenmiller_report, market_regime


class DruckenmillerReportTest(unittest.TestCase):
    def test_market_regime_uses_breadth(self):
        self.assertEqual(market_regime(13)["state"], "RiskOn")
        self.assertEqual(market_regime(9)["state"], "Selective")
        self.assertEqual(market_regime(5)["state"], "Tactical")
        self.assertEqual(market_regime(2)["state"], "Defense")

    def test_combines_momentum_and_active_etf_signals(self):
        strategies = {
            "momentum": [
                {
                    "code": "2330.TW",
                    "name": "台積電",
                    "price": 1200,
                    "date": "2026-06-18",
                    "score": 10,
                    "fundamentals": {"rev_yoy": 0.22, "growth": 0.31, "pe": 24},
                }
            ],
            "active_etf": [
                {
                    "code": "2330",
                    "name": "台積電",
                    "industry": "半導體業",
                    "side": "buy",
                    "same_side_count": 2,
                    "etf_count": 2,
                    "buy_count": 2,
                    "sell_count": 0,
                    "net_amount": 200000000,
                    "date": "2026-06-18",
                }
            ],
        }
        report = generate_druckenmiller_report(strategies, market_breadth=9, target_date="2026-06-18")
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["candidates"][0]["code"], "2330")
        self.assertGreaterEqual(report["candidates"][0]["signalCount"], 2)
        self.assertIn(report["candidates"][0]["conviction"], {"觀察核心", "高信念"})

    def test_empty_report_is_no_data(self):
        report = generate_druckenmiller_report({}, market_breadth=0, target_date="2026-06-18")
        self.assertEqual(report["status"], "no_data")
        self.assertEqual(report["candidates"], [])


if __name__ == "__main__":
    unittest.main()
