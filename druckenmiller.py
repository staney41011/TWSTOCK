from datetime import datetime


def base_code(code):
    return str(code or "").split(".")[0]


def safe_number(value, default=0):
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def as_list(value):
    return value if isinstance(value, list) else []


def market_regime(market_breadth):
    breadth = safe_number(market_breadth, 0)
    if breadth >= 12:
        return {
            "state": "RiskOn",
            "label": "進攻盤",
            "suggested_exposure": "50%～80%",
            "score": 14,
            "description": "創 60 日新高家數擴散，適合把高共振標的放在觀察核心。",
        }
    if breadth >= 8:
        return {
            "state": "Selective",
            "label": "精選盤",
            "suggested_exposure": "30%～55%",
            "score": 9,
            "description": "盤面有機會但不普漲，優先找產業與個股訊號同向者。",
        }
    if breadth >= 4:
        return {
            "state": "Tactical",
            "label": "戰術盤",
            "suggested_exposure": "15%～35%",
            "score": 5,
            "description": "強勢股仍有機會，但需要小部位、快確認、嚴守 thesis break。",
        }
    return {
        "state": "Defense",
        "label": "防守盤",
        "suggested_exposure": "0%～20%",
        "score": 0,
        "description": "市場廣度偏弱，保留現金比追高更重要。",
    }


def fundamental_points(item):
    fundamentals = item.get("fundamentals") or {}
    score = 0
    notes = []

    rev_yoy = safe_number(fundamentals.get("rev_yoy"), None)
    if rev_yoy is not None:
        if rev_yoy >= 0.3:
            score += 12
            notes.append("營收 YoY > 30%")
        elif rev_yoy >= 0.15:
            score += 9
            notes.append("營收 YoY > 15%")
        elif rev_yoy > 0:
            score += 4
            notes.append("營收正成長")
        elif rev_yoy < 0:
            score -= 6
            notes.append("營收 YoY 轉弱")

    growth = safe_number(fundamentals.get("growth"), None)
    if growth is not None:
        if growth >= 0.3:
            score += 8
            notes.append("EPS 高成長")
        elif growth >= 0.15:
            score += 5
            notes.append("EPS 成長")
        elif growth < 0:
            score -= 5
            notes.append("EPS 衰退")

    pe = safe_number(fundamentals.get("pe"), None)
    if pe is not None and 0 < pe < 30:
        score += 3
        notes.append("估值未過熱")
    elif pe is not None and pe >= 60 and pe != 999:
        score -= 4
        notes.append("估值偏高")

    return score, notes


def conviction_label(score):
    if score >= 78:
        return "高信念"
    if score >= 62:
        return "觀察核心"
    if score >= 48:
        return "小部位試單"
    return "只觀察"


def industry_touch(industries, name, score=0, amount=0, source=None):
    if not name:
        return
    item = industries.setdefault(name, {
        "name": name,
        "score": 0,
        "netAmount": 0,
        "signals": 0,
        "sources": [],
    })
    item["score"] += score
    item["netAmount"] += safe_number(amount)
    item["signals"] += 1
    if source and source not in item["sources"]:
        item["sources"].append(source)


def candidate_touch(candidates, code, name=None, industry=None, price=None, date=None, source=None, points=0, thesis=None, warning=None, item=None):
    clean = base_code(code)
    if not clean:
        return None

    candidate = candidates.setdefault(clean, {
        "code": clean,
        "name": name or clean,
        "industry": industry,
        "price": price,
        "date": date,
        "score": 0,
        "scoreParts": [],
        "signals": [],
        "warnings": [],
        "thesis": [],
        "fundamentals": {},
    })

    if name and candidate["name"] == clean:
        candidate["name"] = name
    if industry and not candidate.get("industry"):
        candidate["industry"] = industry
    if price is not None and candidate.get("price") is None:
        candidate["price"] = price
    if date and not candidate.get("date"):
        candidate["date"] = date
    if item and item.get("fundamentals") and not candidate.get("fundamentals"):
        candidate["fundamentals"] = item.get("fundamentals") or {}

    candidate["score"] += points
    if points:
        candidate["scoreParts"].append({"source": source or "signal", "points": round(points, 1)})
    if source and source not in candidate["signals"]:
        candidate["signals"].append(source)
    if thesis:
        candidate["thesis"].append(thesis)
    if warning:
        candidate["warnings"].append(warning)
    return candidate


def add_holy_grail_candidates(strategies, candidates, industries):
    holy = strategies.get("holy_grail") or {}
    buckets = holy.get("candidates") or {}
    weights = {
        "breakout": (22, "聖杯突破"),
        "pullback": (15, "聖杯回檔轉強"),
        "overheated": (-8, "聖杯過熱"),
        "exit": (-24, "聖杯出場警示"),
    }
    for bucket, rows in buckets.items():
        points, label = weights.get(bucket, (0, bucket))
        for item in as_list(rows):
            industry = item.get("industry")
            industry_touch(industries, industry, points / 2, 0, label)
            candidate_touch(
                candidates,
                item.get("code"),
                item.get("name"),
                industry=industry,
                price=item.get("close") or item.get("price"),
                date=item.get("date"),
                source=label,
                points=points,
                thesis=item.get("signal") or label,
                warning="聖杯已列出出場警示" if bucket == "exit" else None,
                item=item,
            )


def generate_druckenmiller_report(strategies, market_breadth=0, target_date=None):
    regime = market_regime(market_breadth)
    candidates = {}
    industries = {}

    for item in as_list(strategies.get("momentum")):
        points = 26 + min(safe_number(item.get("score")) * 2, 18)
        industry = item.get("industry")
        fundamental_score, notes = fundamental_points(item)
        industry_touch(industries, industry, 12, 0, "動能突破")
        candidate = candidate_touch(
            candidates,
            item.get("code"),
            item.get("name"),
            industry=industry,
            price=item.get("price"),
            date=item.get("date"),
            source="動能突破",
            points=points,
            thesis="創高後仍有基本面支撐",
            item=item,
        )
        if candidate:
            candidate["score"] += fundamental_score
            if fundamental_score:
                candidate["scoreParts"].append({"source": "基本面改善", "points": round(fundamental_score, 1)})
            candidate["thesis"].extend(notes)

    for item in as_list(strategies.get("macd_turn_red")):
        day = safe_number(item.get("macd_day"), 3)
        points = {1: 16, 2: 11, 3: 8}.get(int(day), 6)
        fundamental_score, notes = fundamental_points(item)
        candidate = candidate_touch(
            candidates,
            item.get("code"),
            item.get("name"),
            price=item.get("price"),
            date=item.get("date"),
            source="MACD 翻紅",
            points=points,
            thesis=item.get("pattern") or "MACD 柱狀體翻紅",
            item=item,
        )
        if candidate:
            candidate["score"] += max(-4, min(8, fundamental_score / 2))
            candidate["thesis"].extend(notes[:2])

    for item in as_list(strategies.get("active_etf")):
        side = item.get("side")
        if side == "buy":
            points = 26 + min(safe_number(item.get("same_side_count")) * 4, 12)
            thesis = "主動 ETF 同向加碼"
            warning = None
        elif side == "mixed":
            points = 9
            thesis = "主動 ETF 多空分歧但有資金流"
            warning = "ETF 流向分歧，需等方向確認"
        else:
            points = -18
            thesis = "主動 ETF 偏賣"
            warning = "主動 ETF 轉向賣超"

        amount = safe_number(item.get("net_amount"))
        points += max(-8, min(10, amount / 100000000 * 2))
        industry = item.get("industry")
        industry_touch(industries, industry, points / 2, amount, "主動 ETF")
        candidate = candidate_touch(
            candidates,
            item.get("code"),
            item.get("name"),
            industry=industry,
            date=item.get("date"),
            source="主動 ETF",
            points=points,
            thesis=thesis,
            warning=warning,
            item=item,
        )
        if candidate:
            candidate["activeEtf"] = {
                "side": side,
                "etfCount": item.get("etf_count"),
                "buyCount": item.get("buy_count"),
                "sellCount": item.get("sell_count"),
                "netAmount": item.get("net_amount"),
            }

    for item in as_list(strategies.get("cbas")):
        candidate_touch(
            candidates,
            item.get("code"),
            item.get("name"),
            price=item.get("price"),
            date=item.get("date"),
            source="CBAS",
            points=8,
            thesis="可轉債雙低提供非線性觀察點",
            item=item,
        )

    for item in as_list(strategies.get("doji_rise")):
        candidate_touch(
            candidates,
            item.get("code"),
            item.get("name"),
            price=item.get("price"),
            date=item.get("date"),
            source="十字星整理",
            points=6,
            thesis=item.get("pattern") or "多頭整理後觀察轉強",
            item=item,
        )

    add_holy_grail_candidates(strategies, candidates, industries)

    rows = []
    for candidate in candidates.values():
        candidate["score"] += regime["score"]
        candidate["scoreParts"].append({"source": "大盤環境", "points": regime["score"]})
        candidate["score"] = round(max(0, min(100, candidate["score"])), 1)
        candidate["conviction"] = conviction_label(candidate["score"])
        candidate["signalCount"] = len(candidate["signals"])
        candidate["thesis"] = list(dict.fromkeys(candidate["thesis"]))[:5]
        candidate["warnings"] = list(dict.fromkeys(candidate["warnings"]))[:4]
        candidate["thesisBreak"] = [
            "主動 ETF 流向轉為連續賣超",
            "MACD 柱狀體翻回綠色或突破失敗",
            "營收 / EPS 成長 thesis 消失",
        ]
        if candidate["score"] >= 38 or candidate["signalCount"] >= 2:
            rows.append(candidate)

    rows.sort(key=lambda item: (item["score"], item["signalCount"]), reverse=True)

    industry_rows = sorted(
        (
            {
                "name": item["name"],
                "score": round(item["score"], 1),
                "netAmount": round(item["netAmount"], 0),
                "signals": item["signals"],
                "sources": item["sources"][:4],
            }
            for item in industries.values()
        ),
        key=lambda item: (item["score"], item["netAmount"]),
        reverse=True,
    )[:8]

    status = "ok" if rows else "no_data"
    return {
        "title": "Druckenmiller 風格雷達",
        "subtitle": "先看大盤與產業，再找高信念、可快速驗證 thesis 的股票。",
        "disclaimer": "本工具僅把公開投資哲學轉成量化篩選，僅供研究與教育用途，不構成投資建議。",
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "targetDate": target_date,
        "status": status,
        "market": {
            **regime,
            "breadth": safe_number(market_breadth, 0),
        },
        "industries": industry_rows,
        "candidates": rows[:30],
        "principles": [
            "大盤流動性與廣度決定倉位，不在弱盤硬做大部位。",
            "集中在少數高信念標的，訊號越多越值得追蹤。",
            "追蹤未來 12～24 個月可能改善的產業與公司，而不是只看現在。",
            "買進理由消失就出場，不用用虧損幅度替 thesis 做決策。",
        ],
        "sourceNotes": [
            "大盤環境使用 60 日新高家數占比近似市場廣度。",
            "產業與資金共振來自動能、MACD、主動 ETF、CBAS 與聖杯策略結果。",
        ],
    }
