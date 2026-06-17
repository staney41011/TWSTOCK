import unittest

from main import find_etfinfo_active_summary


class ActiveEtfSummaryTest(unittest.TestCase):
    def test_finds_legacy_summary_key(self):
        summary = {"etfs": [], "flowRankings": []}
        data = {"active-summary-weekly-0": summary}
        self.assertIs(find_etfinfo_active_summary(data), summary)

    def test_finds_market_scoped_summary_key(self):
        summary = {
            "etfs": [{"code": "00981A", "name": "主動統一台股增長"}],
            "flowRankings": [{"stockCode": "2454", "etfDetails": []}],
        }
        data = {
            "active-stock-list-compact": {"items": [], "updatedAt": "2026-06-14"},
            "active-summary-weekly-0-market-0": summary,
        }
        self.assertIs(find_etfinfo_active_summary(data), summary)

    def test_ignores_non_summary_payloads(self):
        data = {"active-stock-list-compact": {"items": [], "updatedAt": "2026-06-14"}}
        self.assertIsNone(find_etfinfo_active_summary(data))


if __name__ == "__main__":
    unittest.main()
