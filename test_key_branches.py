import unittest

from key_branches import (
    aggregate_branch_rows,
    branch_signal_for_stock,
    generate_key_branch_report,
    strategy_contexts,
)


class KeyBranchesTest(unittest.TestCase):
    def test_aggregate_branch_rows(self):
        rows = [
            {"securities_trader_id": "9200", "securities_trader": "凱基-台北", "price": 100, "buy": 3000, "sell": 1000},
            {"securities_trader_id": "9200", "securities_trader": "凱基-台北", "price": 101, "buy": 2000, "sell": 0},
            {"securities_trader_id": "9800", "securities_trader": "元大", "price": 100, "buy": 0, "sell": 4000},
        ]
        branches = aggregate_branch_rows(rows)
        kgi = next(item for item in branches if item["branchId"] == "9200")
        self.assertEqual(kgi["buyLots"], 5)
        self.assertEqual(kgi["sellLots"], 1)
        self.assertEqual(kgi["netLots"], 4)

    def test_branch_signal_bias(self):
        context = {
            "code": "2330",
            "name": "台積電",
            "price": 1000,
            "pctChange": 1.2,
            "strategyScore": 30,
            "sources": ["台股聖杯", "主動ETF"],
        }
        rows = [
            {"securities_trader_id": "9200", "securities_trader": "凱基-台北", "price": 1000, "buy": 150000, "sell": 10000},
            {"securities_trader_id": "9800", "securities_trader": "元大", "price": 1001, "buy": 70000, "sell": 5000},
            {"securities_trader_id": "8880", "securities_trader": "摩根大通", "price": 999, "buy": 0, "sell": 25000},
        ]
        signal = branch_signal_for_stock(context, rows)
        self.assertEqual(signal["bias"], "偏多")
        self.assertGreater(signal["branchScore"], 55)
        self.assertEqual(signal["buyers"][0]["branchName"], "凱基-台北")

    def test_strategy_contexts_and_no_token_report(self):
        strategies = {
            "macd_turn_red": [{"code": "2330.TW", "name": "台積電", "price": 1000}],
            "active_etf": [{"code": "2330", "name": "台積電", "side": "buy"}],
        }
        contexts = strategy_contexts(strategies)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["strategyScore"], 30)
        report = generate_key_branch_report(strategies, "2026-06-15", token="")
        self.assertEqual(report["status"], "needs_token")
        self.assertEqual(report["targets"], 1)


if __name__ == "__main__":
    unittest.main()
