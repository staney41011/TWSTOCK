import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests


FINMIND_BRANCH_URL = "https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report"


def base_code(code):
    return str(code or "").split(".")[0]


def safe_number(value, default=0):
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def empty_key_branch_report(reason="尚未設定分點資料來源。"):
    return {
        "title": "關鍵分點",
        "subtitle": "追蹤高共振股票的主買分點、主賣分點與分點籌碼偏向。",
        "status": "needs_token",
        "source": "FinMind TaiwanStockTradingDailyReport",
        "sourceNote": reason,
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": None,
        "targets": 0,
        "items": [],
    }


def strategy_contexts(strategies, max_targets=36):
    contexts = {}

    def touch(code, name=None, price=None, pct_change=None, source=None, weight=0, note=None):
        clean = base_code(code)
        if not clean:
            return
        item = contexts.setdefault(clean, {
            "code": clean,
            "name": name or clean,
            "price": price,
            "pctChange": pct_change,
            "strategyScore": 0,
            "sources": [],
            "notes": [],
        })
        if name and item["name"] == clean:
            item["name"] = name
        if price is not None and item.get("price") is None:
            item["price"] = price
        if pct_change is not None and item.get("pctChange") is None:
            item["pctChange"] = pct_change
        if source and source not in item["sources"]:
            item["sources"].append(source)
        if note:
            item["notes"].append(note)
        item["strategyScore"] += weight

    for item in strategies.get("momentum", [])[:12]:
        touch(item.get("code"), item.get("name"), item.get("price"), item.get("pct_change"), "動能爆發", 10)
    for item in strategies.get("macd_turn_red", [])[:16]:
        touch(item.get("code"), item.get("name"), item.get("price"), item.get("pct_change"), "MACD翻紅", 12, item.get("pattern"))
    for item in strategies.get("cbas", [])[:12]:
        touch(item.get("code"), item.get("name"), item.get("price"), item.get("pct_change"), "CBAS", 14)
    for item in strategies.get("active_etf", [])[:20]:
        weight = 18 if item.get("side") == "buy" else 8 if item.get("side") == "mixed" else -6
        touch(item.get("code"), item.get("name"), None, None, "主動ETF", weight, item.get("industry"))

    holy = strategies.get("holy_grail") or {}
    candidates = holy.get("candidates") or {}
    for bucket, rows in candidates.items():
        bucket_weight = {"breakout": 22, "pullback": 18, "overheated": -8, "exit": -14}.get(bucket, 0)
        for item in rows[:12]:
            touch(item.get("code"), item.get("name"), item.get("close") or item.get("price"), None, "台股聖杯", bucket_weight, item.get("signal"))

    ranked = sorted(contexts.values(), key=lambda item: item["strategyScore"], reverse=True)
    return ranked[:max_targets]


def fetch_finmind_branch_rows(stock_id, date, token=None, timeout=20):
    headers = {"User-Agent": "Mozilla/5.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {"data_id": base_code(stock_id), "date": date}
    response = requests.get(FINMIND_BRANCH_URL, headers=headers, params=params, timeout=timeout)
    payload = response.json()
    if response.status_code != 200 or payload.get("status") not in (200, "200", None):
        raise RuntimeError(payload.get("msg") or f"HTTP {response.status_code}")
    return payload.get("data") or []


def aggregate_branch_rows(rows):
    branches = {}
    for row in rows:
        branch_id = str(row.get("securities_trader_id") or row.get("dealer_id") or "")
        name = row.get("securities_trader") or row.get("dealer_name") or branch_id or "未知分點"
        buy = safe_number(row.get("buy"))
        sell = safe_number(row.get("sell"))
        price = safe_number(row.get("price"), None)
        key = branch_id or name
        item = branches.setdefault(key, {
            "branchId": branch_id,
            "branchName": name,
            "buyShares": 0,
            "sellShares": 0,
            "turnoverValue": 0,
        })
        item["buyShares"] += buy
        item["sellShares"] += sell
        if price:
            item["turnoverValue"] += (buy + sell) * price

    result = []
    for item in branches.values():
        buy_lots = item["buyShares"] / 1000
        sell_lots = item["sellShares"] / 1000
        net_lots = buy_lots - sell_lots
        gross_lots = buy_lots + sell_lots
        result.append({
            "branchId": item["branchId"],
            "branchName": item["branchName"],
            "buyLots": round(buy_lots, 1),
            "sellLots": round(sell_lots, 1),
            "netLots": round(net_lots, 1),
            "grossLots": round(gross_lots, 1),
            "avgPrice": round(item["turnoverValue"] / ((item["buyShares"] + item["sellShares"]) or 1), 2) if item["turnoverValue"] else None,
        })
    return result


def branch_signal_for_stock(context, rows):
    branches = aggregate_branch_rows(rows)
    if not branches:
        return None

    buyers = sorted([item for item in branches if item["netLots"] > 0], key=lambda item: item["netLots"], reverse=True)[:5]
    sellers = sorted([item for item in branches if item["netLots"] < 0], key=lambda item: item["netLots"])[:5]
    top_buy = sum(item["netLots"] for item in buyers[:3])
    top_sell = abs(sum(item["netLots"] for item in sellers[:3]))
    total_gross = sum(item["grossLots"] for item in branches) or 1
    net_pressure = top_buy - top_sell
    concentration = max(top_buy, top_sell) / total_gross
    chip_points = min(45, max(top_buy, top_sell) / 120 * 45)
    concentration_points = min(25, concentration * 220)
    strategy_points = max(-20, min(30, context.get("strategyScore", 0)))
    score = round(max(0, min(100, chip_points + concentration_points + strategy_points)), 1)

    if net_pressure > 0 and score >= 55:
        bias = "偏多"
    elif net_pressure < 0 and score >= 55:
        bias = "偏空"
    else:
        bias = "分歧"

    warnings = []
    pct = context.get("pctChange")
    if pct is not None:
        if net_pressure > 0 and safe_number(pct) <= 0:
            warnings.append("主買明顯但股價未跟漲")
        if net_pressure < 0 and safe_number(pct) >= 0:
            warnings.append("主賣明顯但股價未轉弱")

    return {
        "code": context["code"],
        "name": context.get("name") or context["code"],
        "price": context.get("price"),
        "pctChange": pct,
        "bias": bias,
        "branchScore": score,
        "strategyScore": round(context.get("strategyScore", 0), 1),
        "strategySources": context.get("sources", []),
        "topBuyNetLots": round(top_buy, 1),
        "topSellNetLots": round(top_sell, 1),
        "netPressureLots": round(net_pressure, 1),
        "concentrationPct": round(concentration * 100, 2),
        "buyers": buyers,
        "sellers": sellers,
        "warnings": warnings,
    }


def generate_key_branch_report(strategies, target_date, token=None, max_targets=36, max_workers=8):
    token = token if token is not None else os.getenv("FINMIND_TOKEN")
    if not token:
        report = empty_key_branch_report("需要在 GitHub Secrets 或本機環境設定 FINMIND_TOKEN，才能抓取券商分點資料。")
        report["date"] = target_date
        report["targets"] = len(strategy_contexts(strategies, max_targets=max_targets))
        return report

    targets = strategy_contexts(strategies, max_targets=max_targets)
    items = []
    errors = []

    def load(context):
        rows = fetch_finmind_branch_rows(context["code"], target_date, token=token)
        signal = branch_signal_for_stock(context, rows)
        return signal

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(load, context): context for context in targets}
        for future in as_completed(futures):
            context = futures[future]
            try:
                item = future.result()
                if item:
                    items.append(item)
            except Exception as exc:
                errors.append({"code": context["code"], "name": context.get("name"), "message": str(exc)})

    items.sort(key=lambda item: (item["branchScore"], abs(item["netPressureLots"])), reverse=True)
    status = "ok" if items else "no_data"
    return {
        "title": "關鍵分點",
        "subtitle": "追蹤高共振股票的主買分點、主賣分點與分點籌碼偏向。",
        "status": status,
        "source": "FinMind TaiwanStockTradingDailyReport",
        "sourceNote": "分點資料需 FINMIND_TOKEN；資料通常於盤後更新，實際時間以來源為準。",
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": target_date,
        "targets": len(targets),
        "items": items,
        "errors": errors[:10],
    }
