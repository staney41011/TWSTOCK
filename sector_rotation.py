"""台股產業輪動核心：使用既有個股歷史行情，計算相對強弱、動能、廣度和量能。

所有產業報酬採樣本股票等權平均；成交值由收盤價 x 成交股數估算，
不是法人實際買賣超或資金淨流入。盤後快照只供研究，非投資建議。
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import math

HORIZONS = (1, 5, 10, 20, 60)
LOOKBACK_BARS = 125
REPLAY_DAYS = 24
MIN_MEMBERS = 3


def finite_number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def snapshot_stock(ticker, name, industry, dataframe):
    """擷取小型序列，不另外抓資料；忽略無效收盤價及非股票類別。"""
    if not industry or dataframe is None or len(dataframe) < 70:
        return None
    bars = []
    for date, row in dataframe.tail(LOOKBACK_BARS).iterrows():
        price = finite_number(row.get("Close"))
        volume = finite_number(row.get("Volume"))
        if price is None or price <= 0:
            continue
        bars.append([date.strftime("%Y-%m-%d"), round(price, 4), max(0, volume or 0)])
    if len(bars) < 70:
        return None
    return {
        "code": str(ticker).split(".")[0],
        "name": str(name or ticker),
        "industry": str(industry).strip(),
        "bars": bars,
    }


def snapshot_benchmark(dataframe):
    if dataframe is None:
        return []
    bars = []
    for date, row in dataframe.tail(LOOKBACK_BARS).iterrows():
        close = finite_number(row.get("Close"))
        if close is not None and close > 0:
            bars.append([date.strftime("%Y-%m-%d"), round(close, 4)])
    return bars


def pct(start, end):
    if start is None or end is None or start <= 0:
        return None
    return (end / start - 1.0) * 100.0


def average(values):
    cleaned = [value for value in values if value is not None and math.isfinite(value)]
    return sum(cleaned) / len(cleaned) if cleaned else None


def clip(value, low, high):
    return max(low, min(high, value))


def valid_returns(lookups, start_date, end_date):
    result = []
    for stock, prices, volumes in lookups:
        ret = pct(prices.get(start_date), prices.get(end_date))
        if ret is not None:
            result.append((stock, ret, prices, volumes))
    return result


def build_sector_rotation_report(stocks, benchmark_bars=None, target_date=None,
                                 min_members=MIN_MEMBERS, replay_days=REPLAY_DAYS):
    """以共同交易日期比較，且回放日的計算只使用該日及以前資料，避免前視偏差。"""
    grouped = defaultdict(list)
    all_lookups = []
    counts = Counter()
    for stock in stocks or []:
        if not isinstance(stock, dict) or not stock.get("industry"):
            continue
        bars = stock.get("bars") or []
        prices = {}
        volumes = {}
        for entry in bars:
            if not isinstance(entry, (list, tuple)) or len(entry) < 3:
                continue
            date, close, volume = entry[:3]
            close, volume = finite_number(close), finite_number(volume)
            if close is None or close <= 0:
                continue
            prices[str(date)] = close
            volumes[str(date)] = max(0, volume or 0) * close
        if len(prices) < 70:
            continue
        item = (stock, prices, volumes)
        grouped[stock["industry"]].append(item)
        all_lookups.append(item)
        counts.update(prices.keys())

    now = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    report = {
        "version": 1,
        "generated_at": now,
        "as_of": None,
        "requested_date": target_date,
        "benchmark": {"name": "未取得", "source": "none"},
        "methodology": {
            "classification": "twstock 證券產業分類；排除 ETF、非股票及資料不足標的",
            "return": "同產業有效樣本股票之簡單報酬等權平均，非市值加權官方產業指數",
            "x": "選擇期間的產業平均報酬 - 比較基準報酬（百分點）",
            "y": "最近 5 日相對報酬 - 前 5 日相對報酬（百分點）",
            "breadth": "近 5 日報酬 > 0 的樣本股票比率",
            "turnover": "由收盤價乘成交股數估算的成交值，並非淨資金流入",
            "volume_ratio": "近 5 日平均估算成交值 / 近 20 日平均估算成交值",
            "score": "20日相對報酬、5日相對動能、量能變化及上漲廣度的研究用合成分數",
            "warning": "非投資建議；樣本及資料來源缺漏會造成偏差，歷史象限並非交易績效回測",
        },
        "coverage": {"stocks": len(all_lookups), "industries": 0, "min_members": min_members},
        "days": [],
    }
    if not counts:
        return report

    # 某些股票有停牌 / 延遲資料；避免由單一股票製造「未來交易日」。
    threshold = max(3, int(len(all_lookups) * 0.15))
    dates = sorted(date for date, count in counts.items() if count >= threshold)
    if not dates:
        return report
    dates = dates[-LOOKBACK_BARS:]
    report["as_of"] = dates[-1]

    bench_prices = {}
    for bar in benchmark_bars or []:
        if isinstance(bar, (list, tuple)) and len(bar) >= 2:
            value = finite_number(bar[1])
            if value is not None and value > 0:
                bench_prices[str(bar[0])] = value

    # 整個回放視窗的指標應使用相同基準，避免同圖混入不同基準。
    required_dates = dates[max(0, len(dates) - replay_days - 65):]
    official = bool(required_dates) and sum(d in bench_prices for d in required_dates) / len(required_dates) >= 0.98
    if official:
        report["benchmark"] = {"name": "台灣加權指數 (^TWII)", "source": "yfinance"}
    else:
        report["benchmark"] = {"name": "有效樣本股票等權比較", "source": "sample_equal_weight"}

    def base_return(first, last):
        if official:
            return pct(bench_prices.get(first), bench_prices.get(last))
        return average([ret for _, ret, _, _ in valid_returns(all_lookups, first, last)])

    start_idx = max(65, len(dates) - replay_days)
    for idx in range(start_idx, len(dates)):
        today, d5, d10, d20 = dates[idx], dates[idx - 5], dates[idx - 10], dates[idx - 20]
        bases = {str(h): base_return(dates[idx - h], today) for h in HORIZONS}
        prev5_base = base_return(d10, d5)
        items = []
        for industry, lookups in grouped.items():
            rows20 = valid_returns(lookups, d20, today)
            rows5 = valid_returns(lookups, d5, today)
            previous5 = valid_returns(lookups, d10, d5)
            if len(rows20) < min_members or len(rows5) < min_members or len(previous5) < min_members:
                continue

            relative = {}
            returns = {}
            for h in HORIZONS:
                group_ret = average([r for _, r, _, _ in valid_returns(lookups, dates[idx - h], today)])
                returns[str(h)] = round(group_ret, 3) if group_ret is not None else None
                relative[str(h)] = round(group_ret - bases[str(h)], 3) if group_ret is not None and bases[str(h)] is not None else None
            rel20, rel5 = relative["20"], relative["5"]
            prior_rel5 = average([r for _, r, _, _ in previous5])
            if prior_rel5 is not None and prev5_base is not None:
                prior_rel5 -= prev5_base
            else:
                prior_rel5 = None
            if rel20 is None or rel5 is None or prior_rel5 is None:
                continue

            acceleration = round(rel5 - prior_rel5, 3)
            breadth = round(100 * sum(r > 0 for _, r, _, _ in rows5) / len(rows5), 1)
            # 使用當天具備 20 日價格的固定成分，避免轉倉 / 停牌干擾。
            constituent_value = {}
            for stock, _ret, prices, vols in rows20:
                constituent_value[stock["code"]] = vols
            recent_values = []
            for date in dates[idx - 19:idx + 1]:
                recent_values.append(sum(vols.get(date, 0) for vols in constituent_value.values()))
            avg20 = average(recent_values) or 0
            avg5 = average(recent_values[-5:]) or 0
            volume_ratio = round(avg5 / avg20, 3) if avg20 > 0 else None
            score = clip(
                50 + clip(rel20 * 2.3, -24, 24)
                + clip(acceleration * 2, -14, 14)
                + clip(((volume_ratio or 1) - 1) * 12, -10, 10)
                + clip((breadth - 50) * 0.2, -10, 10),
                0, 100,
            )
            if rel20 >= 0 and acceleration >= 0:
                state = "領先"
            elif rel20 >= 0:
                state = "減弱中"
            elif acceleration >= 0:
                state = "改善中"
            else:
                state = "落後"

            leaders = []
            for stock, ret5, prices, volumes in rows5:
                close = prices.get(today)
                r20 = pct(prices.get(d20), close)
                leaders.append({
                    "code": stock["code"],
                    "name": stock.get("name") or stock["code"],
                    "return5": round(ret5, 2),
                    "return20": round(r20, 2) if r20 is not None else None,
                    "close": close,
                    "turnover": round(volumes.get(today, 0)),
                })
            leaders.sort(key=lambda s: s["return5"], reverse=True)
            items.append({
                "name": industry,
                "state": state,
                "strength": round(rel20, 3),
                "momentum": acceleration,
                "returns": returns,
                "relative": relative,
                "breadth": breadth,
                "volume_ratio": volume_ratio,
                "turnover20": round(avg20),
                "score": round(score, 1),
                "count": len(rows20),
                "up_count": sum(r > 0 for _, r, _, _ in rows5),
                "stocks": leaders[:30],
            })
        items.sort(key=lambda item: (-item["score"], item["name"]))
        if items:
            report["days"].append({"date": today, "industries": items, "benchmark_returns": bases})

    if report["days"]:
        report["as_of"] = report["days"][-1]["date"]
        report["coverage"]["industries"] = len(report["days"][-1]["industries"])
    return report
