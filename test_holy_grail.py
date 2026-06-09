import unittest
from datetime import date, timedelta

from holy_grail import (
    RiskConfig,
    calculate_position_size,
    calculate_sma,
    detect_breakout,
    generate_taiwan_holy_grail_report,
    get_market_regime,
)


def make_bars(closes, volumes=None):
    start = date(2025, 1, 1)
    volumes = volumes or [1000] * len(closes)
    bars = []
    for index, close in enumerate(closes):
        bars.append({
            "date": (start + timedelta(days=index)).strftime("%Y-%m-%d"),
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": volumes[index],
        })
    return bars


class HolyGrailStrategyTest(unittest.TestCase):
    def test_calculate_sma(self):
        self.assertEqual(calculate_sma([1, 2, 3], 2), 2.5)
        self.assertIsNone(calculate_sma([1, 2, 3], 5))

    def test_market_regime_bull_and_bear(self):
        bull = make_bars([100 + index for index in range(130)])
        self.assertEqual(get_market_regime(bull)["state"], "Bull")

        bear = make_bars([100] * 129 + [80])
        self.assertEqual(get_market_regime(bear)["state"], "Bear")

    def test_detect_breakout(self):
        closes = [100 + index for index in range(21)] + [123]
        volumes = [1000] * 21 + [2200]
        self.assertTrue(detect_breakout(make_bars(closes, volumes)))

    def test_position_size_respects_single_position_cap(self):
        config = RiskConfig(
            capital=1_000_000,
            risk_per_trade=0.01,
            max_single_position=0.20,
            max_industry_exposure=0.40,
        )
        result = calculate_position_size(config, entry_price=100, stop_price=95)
        self.assertTrue(result["valid"])
        self.assertEqual(result["positionValue"], 200000)
        self.assertEqual(result["shares"], 2000)

    def test_generate_report_from_mock_data(self):
        market_bars = make_bars([100 + index for index in range(130)])
        stock_closes = [80 + index * 0.3 for index in range(129)] + [123]
        stock_bars = make_bars(stock_closes, [1000] * 129 + [2500])
        report = generate_taiwan_holy_grail_report({
            "marketBars": market_bars,
            "industries": {
                "半導體業": [{
                    "code": "2330.TW",
                    "name": "台積電",
                    "industry": "半導體業",
                    "bars": stock_bars,
                }],
            },
            "targetDate": "2025-05-10",
        })
        self.assertEqual(report["market"]["state"], "Bull")
        self.assertEqual(report["industries"][0]["industry"], "半導體業")
        self.assertGreaterEqual(len(report["candidates"]["breakout"]), 1)


if __name__ == "__main__":
    unittest.main()
