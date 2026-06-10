import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import twstock
import yfinance as yf


@dataclass
class RiskConfig:
    capital: float = 1_000_000
    risk_per_trade: float = 0.01
    max_single_position: float = 0.20
    max_industry_exposure: float = 0.40
    stop_mode: str = "MA20"


FINE_INDUSTRY_RULES = [
    {"industry": "半導體-晶圓代工", "bases": {"半導體業"}, "codes": {"2303", "2330", "5347", "6770"}, "keywords": ["台積電", "聯電", "世界", "力積電"]},
    {"industry": "半導體-IC設計", "bases": {"半導體業"}, "codes": {"2379", "2454", "3034", "3443", "3529", "3661", "4919", "4966", "5269", "5274", "6531", "6669"}, "keywords": ["聯發科", "瑞昱", "聯詠", "創意", "智原", "譜瑞", "祥碩", "世芯", "力旺"]},
    {"industry": "半導體-封測", "bases": {"半導體業"}, "codes": {"2449", "3264", "3711", "6239", "6257", "8150"}, "keywords": ["日月光", "京元", "力成", "矽格", "南茂", "欣銓"]},
    {"industry": "半導體-設備材料", "bases": {"半導體業"}, "codes": {"1560", "3131", "3413", "3583", "3680", "4770", "6196", "6510", "6643"}, "keywords": ["中砂", "弘塑", "辛耘", "家登", "帆宣", "洋基", "均豪", "萬潤"]},
    {"industry": "半導體-矽晶圓", "bases": {"半導體業"}, "codes": {"3016", "3532", "3707", "5483", "6182", "6488"}, "keywords": ["嘉晶", "台勝科", "中美晶", "合晶", "環球晶"]},
    {"industry": "電子零組件-PCB", "bases": {"電子零組件業"}, "codes": {"2313", "2368", "2383", "3037", "3189", "3533", "4958", "6191", "6269", "6274"}, "keywords": ["華通", "金像電", "健鼎", "欣興", "台光電", "臻鼎", "定穎", "楠梓電"]},
    {"industry": "電子零組件-被動元件", "bases": {"電子零組件業"}, "codes": {"2327", "2492", "3026", "6173", "6207"}, "keywords": ["國巨", "華新科", "禾伸堂", "信昌電"]},
    {"industry": "電子零組件-連接器線束", "bases": {"電子零組件業"}, "codes": {"2392", "3003", "3015", "3023", "3324", "3653", "3665", "4915"}, "keywords": ["正崴", "信邦", "台達電", "嘉澤", "貿聯", "良維"]},
    {"industry": "電子零組件-電源散熱", "bases": {"電子零組件業"}, "codes": {"2308", "3017", "3324", "3653", "6230", "6278", "6412"}, "keywords": ["台達電", "奇鋐", "雙鴻", "建準", "尼得科"]},
    {"industry": "電腦週邊-伺服器AI", "bases": {"電腦及週邊設備業"}, "codes": {"2356", "2376", "2382", "3017", "3231", "6669"}, "keywords": ["英業達", "技嘉", "廣達", "緯創", "奇鋐", "緯穎"]},
    {"industry": "電腦週邊-品牌通路", "bases": {"電腦及週邊設備業"}, "codes": {"2353", "2357", "2377", "2395"}, "keywords": ["宏碁", "華碩", "微星", "研華"]},
    {"industry": "光電-面板", "bases": {"光電業"}, "codes": {"2409", "3481", "6116"}, "keywords": ["友達", "群創", "彩晶"]},
    {"industry": "光電-光學鏡頭", "bases": {"光電業"}, "codes": {"3008", "3019", "3406", "3362"}, "keywords": ["大立光", "亞光", "玉晶光", "先進光"]},
    {"industry": "光電-LED", "bases": {"光電業"}, "codes": {"2393", "2448", "2499", "3031", "3714"}, "keywords": ["億光", "晶電", "隆達", "聯嘉"]},
    {"industry": "通信網路-網通設備", "bases": {"通信網路業"}, "codes": {"2345", "2419", "3025", "4906", "5388", "6285"}, "keywords": ["智邦", "仲琦", "星通", "正文", "中磊", "啟碁"]},
    {"industry": "通信網路-射頻天線", "bases": {"通信網路業"}, "codes": {"2455", "3596", "4908", "4977", "6284"}, "keywords": ["全新", "聯德控股", "前鼎", "眾達", "佳邦"]},
    {"industry": "資訊服務-資安雲端", "bases": {"資訊服務業"}, "codes": {"2480", "3029", "5203", "6214", "6590", "6680"}, "keywords": ["敦陽科", "零壹", "訊連", "精誠", "安碁", "鑫創"]},
    {"industry": "金融-金控銀行", "bases": {"金融保險業"}, "keywords": ["金", "銀行", "合庫", "彰銀", "臺企銀"]},
    {"industry": "金融-證券期貨", "bases": {"金融保險業"}, "keywords": ["證", "期"]},
    {"industry": "金融-保險", "bases": {"金融保險業"}, "keywords": ["壽", "產險"]},
    {"industry": "電機機械-工具機", "bases": {"電機機械"}, "keywords": ["上銀", "亞德客", "直得", "程泰", "東台", "協易機"]},
    {"industry": "電機機械-自動化設備", "bases": {"電機機械"}, "keywords": ["盟立", "廣運", "均豪", "鈞興", "氣立"]},
    {"industry": "生技醫療-製藥", "bases": {"生技醫療業"}, "keywords": ["藥", "生技", "製藥"]},
    {"industry": "生技醫療-醫材通路", "bases": {"生技醫療業"}, "keywords": ["醫材", "醫療", "保瑞", "大樹"]},
    {"industry": "航運-貨櫃", "bases": {"航運業"}, "codes": {"2603", "2609", "2615"}, "keywords": ["長榮", "陽明", "萬海"]},
    {"industry": "航運-散裝航空", "bases": {"航運業"}, "keywords": ["裕民", "新興", "慧洋", "華航", "長榮航"]},
    {"industry": "鋼鐵-上游原料", "bases": {"鋼鐵工業"}, "keywords": ["中鋼", "中鴻", "燁輝", "東和"]},
    {"industry": "鋼鐵-不鏽鋼扣件", "bases": {"鋼鐵工業"}, "keywords": ["彰源", "大成鋼", "世豐", "聚亨"]},
]

US_INDUSTRY_ETFS = [
    {"symbol": "SMH", "name": "美股半導體", "mappedIndustries": ["半導體-晶圓代工", "半導體-IC設計", "半導體-封測", "半導體-設備材料", "半導體-矽晶圓"]},
    {"symbol": "XLK", "name": "美股科技", "mappedIndustries": ["半導體-IC設計", "半導體-晶圓代工", "電腦週邊-伺服器AI", "資訊服務-資安雲端", "電子零組件-PCB"]},
    {"symbol": "IGV", "name": "美股軟體", "mappedIndustries": ["資訊服務-資安雲端"]},
    {"symbol": "XLC", "name": "美股通訊服務", "mappedIndustries": ["通信網路-網通設備", "通信網路-射頻天線"]},
    {"symbol": "XLI", "name": "美股工業", "mappedIndustries": ["電機機械-工具機", "電機機械-自動化設備", "航運-散裝航空"]},
    {"symbol": "XLF", "name": "美股金融", "mappedIndustries": ["金融-金控銀行", "金融-證券期貨", "金融-保險"]},
    {"symbol": "XLV", "name": "美股醫療", "mappedIndustries": ["生技醫療-製藥", "生技醫療-醫材通路"]},
    {"symbol": "XLE", "name": "美股能源", "mappedIndustries": ["油電燃氣業", "化學工業", "塑膠工業"]},
    {"symbol": "XLB", "name": "美股原物料", "mappedIndustries": ["鋼鐵-上游原料", "鋼鐵-不鏽鋼扣件", "水泥工業", "玻璃陶瓷"]},
    {"symbol": "XLY", "name": "美股非必需消費", "mappedIndustries": ["汽車工業", "貿易百貨業", "觀光餐旅"]},
    {"symbol": "XLP", "name": "美股民生消費", "mappedIndustries": ["食品工業", "貿易百貨業"]},
]


def clean_stock_code(code):
    return str(code or "").split(".")[0]


def classify_taiwan_industry(code, name, base_industry):
    base = base_industry or "未分類"
    stock_code = clean_stock_code(code)
    stock_name = str(name or "")
    for rule in FINE_INDUSTRY_RULES:
        if base not in rule.get("bases", set()):
            continue
        if stock_code in rule.get("codes", set()):
            return rule["industry"]
        if any(keyword and keyword in stock_name for keyword in rule.get("keywords", [])):
            return rule["industry"]
    return base


def safe_float(value, default=None):
    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def dataframe_to_bars(df):
    bars = []
    if df is None or df.empty:
        return bars
    for idx, row in df.iterrows():
        date = pd.to_datetime(idx)
        if getattr(date, "tzinfo", None):
            date = date.tz_localize(None)
        bars.append({
            "date": date.strftime("%Y-%m-%d"),
            "open": safe_float(row.get("Open")),
            "high": safe_float(row.get("High")),
            "low": safe_float(row.get("Low")),
            "close": safe_float(row.get("Close")),
            "volume": safe_float(row.get("Volume"), 0),
        })
    return [bar for bar in bars if bar["close"] is not None]


def calculate_sma(values, period):
    if period <= 0 or len(values) < period:
        return None
    window = [safe_float(value) for value in values[-period:]]
    if any(value is None for value in window):
        return None
    return sum(window) / period


def calculate_return(bars, days):
    if len(bars) <= days:
        return None
    current = safe_float(bars[-1].get("close"))
    previous = safe_float(bars[-days - 1].get("close"))
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / previous


def calculate_volume_ratio(bars):
    if len(bars) < 20:
        return None
    volumes = [safe_float(bar.get("volume"), 0) for bar in bars]
    volume_ma5 = calculate_sma(volumes, 5)
    volume_ma20 = calculate_sma(volumes, 20)
    if not volume_ma5 or not volume_ma20:
        return None
    return volume_ma5 / volume_ma20


def calculate_relative_strength(stock_return, market_return):
    if stock_return is None or market_return is None:
        return None
    return stock_return - market_return


def get_market_regime(index_bars):
    closes = [bar["close"] for bar in index_bars if bar.get("close") is not None]
    volumes = [safe_float(bar.get("volume"), 0) for bar in index_bars]
    if len(closes) < 120 or len(volumes) < 20:
        return {
            "state": "Unknown",
            "label": "資料不足",
            "suggested_exposure": "0%～10%",
            "description": "大盤資料不足 120 日，暫以防守處理。",
            "risk_note": "等待資料補齊後再提高部位。",
        }
    close = closes[-1]
    ma20 = calculate_sma(closes, 20)
    ma60 = calculate_sma(closes, 60)
    ma120 = calculate_sma(closes, 120)
    volume_ma5 = calculate_sma(volumes, 5)
    volume_ma20 = calculate_sma(volumes, 20)

    if close < ma120:
        state = "Bear"
        label = "空頭防守"
        exposure = "0%～10%"
        description = "指數低於 MA120，資金以防守和等待為主。"
    elif close < ma60 or ma20 < ma60:
        state = "RiskOff"
        label = "風險降低"
        exposure = "10%～30%"
        description = "短中期趨勢轉弱，只保留高勝率或低風險部位。"
    elif close > ma60 and ma20 > ma60 and volume_ma5 >= volume_ma20 * 0.9:
        state = "Bull"
        label = "多頭積極"
        exposure = "70%～90%"
        description = "指數站上 MA60 且短均多頭，量能沒有明顯退潮。"
    else:
        state = "Caution"
        label = "偏多謹慎"
        exposure = "40%～60%"
        description = "指數仍在 MA120 之上，但未完全符合積極多頭條件。"

    return {
        "state": state,
        "label": label,
        "suggested_exposure": exposure,
        "description": description,
        "risk_note": "本工具僅供研究與教育用途，不構成投資建議。",
        "close": round(close, 2),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "ma120": round(ma120, 2),
        "volume_ma5": round(volume_ma5, 0),
        "volume_ma20": round(volume_ma20, 0),
        "volume_status": "量能健康" if volume_ma5 >= volume_ma20 * 0.9 else "量能偏弱",
    }


def rank_score(values, value):
    valid = sorted([item for item in values if item is not None])
    if value is None or not valid:
        return 0
    if len(valid) == 1:
        return 100
    rank = sum(1 for item in valid if item <= value) - 1
    return max(0, min(100, rank / (len(valid) - 1) * 100))


def industry_label(score):
    if score >= 80:
        return "主流強勢"
    if score >= 65:
        return "轉強觀察"
    if score >= 50:
        return "中性"
    return "弱勢避開"


def rank_industries(industries, market_bars):
    market_return20 = calculate_return(market_bars, 20) or 0
    rows = []
    for industry_name, stocks in industries.items():
        stock_metrics = []
        for stock in stocks:
            bars = stock.get("bars", [])
            if len(bars) < 120:
                continue
            ret5 = calculate_return(bars, 5)
            ret20 = calculate_return(bars, 20)
            ret60 = calculate_return(bars, 60)
            vol_ratio = calculate_volume_ratio(bars)
            rs20 = calculate_relative_strength(ret20, market_return20)
            if ret5 is None or ret20 is None or ret60 is None:
                continue
            stock_metrics.append({
                "return5": ret5,
                "return20": ret20,
                "return60": ret60,
                "volumeRatio": vol_ratio or 0,
                "relativeStrength20": rs20 or 0,
            })
        if not stock_metrics:
            continue
        avg = {
            "industry": industry_name,
            "baseIndustries": sorted(set(stock.get("baseIndustry") or stock.get("industry") or "未分類" for stock in stocks)),
            "return5": sum(item["return5"] for item in stock_metrics) / len(stock_metrics),
            "return20": sum(item["return20"] for item in stock_metrics) / len(stock_metrics),
            "return60": sum(item["return60"] for item in stock_metrics) / len(stock_metrics),
            "volumeRatio": sum(item["volumeRatio"] for item in stock_metrics) / len(stock_metrics),
            "relativeStrength20": sum(item["relativeStrength20"] for item in stock_metrics) / len(stock_metrics),
            "stockCount": len(stock_metrics),
        }
        rows.append(avg)

    ret5_values = [row["return5"] for row in rows]
    ret20_values = [row["return20"] for row in rows]
    ret60_values = [row["return60"] for row in rows]
    rs_values = [row["relativeStrength20"] for row in rows]
    volume_values = [row["volumeRatio"] for row in rows]

    for row in rows:
        score = (
            rank_score(ret5_values, row["return5"]) * 0.25
            + rank_score(ret20_values, row["return20"]) * 0.30
            + rank_score(ret60_values, row["return60"]) * 0.20
            + rank_score(rs_values, row["relativeStrength20"]) * 0.15
            + rank_score(volume_values, row["volumeRatio"]) * 0.10
        )
        row["industryScore"] = round(score, 1)
        row["status"] = industry_label(score)

    rows.sort(key=lambda item: item["industryScore"], reverse=True)
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    return rows


def latest_change(bars):
    if len(bars) < 2:
        return None
    prev = bars[-2].get("close")
    close = bars[-1].get("close")
    if not prev:
        return None
    return (close - prev) / prev


def moving_average_map(bars):
    closes = [bar["close"] for bar in bars]
    volumes = [safe_float(bar.get("volume"), 0) for bar in bars]
    return {
        "ma5": calculate_sma(closes, 5),
        "ma10": calculate_sma(closes, 10),
        "ma20": calculate_sma(closes, 20),
        "ma60": calculate_sma(closes, 60),
        "ma120": calculate_sma(closes, 120),
        "volumeMA20": calculate_sma(volumes, 20),
    }


def detect_breakout(stock_bars):
    if len(stock_bars) < 21:
        return False
    ma = moving_average_map(stock_bars)
    close = stock_bars[-1]["close"]
    volume = safe_float(stock_bars[-1].get("volume"), 0)
    high20 = max(bar["high"] for bar in stock_bars[-21:-1] if bar.get("high") is not None)
    change = latest_change(stock_bars) or 0
    return close > high20 and ma["volumeMA20"] and volume > ma["volumeMA20"] * 1.5 and change < 0.07


def detect_pullback_rebound(stock_bars):
    if len(stock_bars) < 25:
        return False
    ma = moving_average_map(stock_bars)
    close = stock_bars[-1]["close"]
    volume = safe_float(stock_bars[-1].get("volume"), 0)
    recent = stock_bars[-5:]
    near_ma = any(
        bar.get("low") is not None
        and (abs(bar["low"] - ma["ma10"]) / ma["ma10"] < 0.025 or abs(bar["low"] - ma["ma20"]) / ma["ma20"] < 0.025)
        for bar in recent
        if ma["ma10"] and ma["ma20"]
    )
    volume_shrink = ma["volumeMA20"] and sum(safe_float(bar.get("volume"), 0) for bar in stock_bars[-5:-1]) / 4 < ma["volumeMA20"]
    regain = ma["ma5"] and (close > ma["ma5"] or close > stock_bars[-2].get("high", close))
    return close > ma["ma20"] and near_ma and volume_shrink and regain and volume >= ma["volumeMA20"] * 0.9


def detect_overheated(stock_bars):
    if len(stock_bars) < 20:
        return False
    ma = moving_average_map(stock_bars)
    close = stock_bars[-1]["close"]
    change = latest_change(stock_bars) or 0
    distance = (close - ma["ma20"]) / ma["ma20"] if ma["ma20"] else 0
    recent_changes = [
        (stock_bars[i]["close"] - stock_bars[i - 1]["close"]) / stock_bars[i - 1]["close"]
        for i in range(max(1, len(stock_bars) - 4), len(stock_bars))
        if stock_bars[i - 1].get("close")
    ]
    return change >= 0.07 or distance > 0.15 or sum(1 for item in recent_changes if item > 0.04) >= 3


def detect_exit_warning(stock_bars):
    if len(stock_bars) < 20:
        return None
    ma = moving_average_map(stock_bars)
    close = stock_bars[-1]["close"]
    lows = [bar["low"] for bar in stock_bars[-21:-1] if bar.get("low") is not None]
    low20 = min(lows) if lows else None
    if low20 and close < low20:
        return {"signal": "停損警示", "action": "出場"}
    if ma["ma20"] and close < ma["ma20"]:
        return {"signal": "出場警示", "action": "出場"}
    if ma["ma10"] and close < ma["ma10"]:
        return {"signal": "減碼警示", "action": "減碼"}
    return None


def calculate_stop_price(stock_bars, stop_mode):
    if not stock_bars:
        return None
    close = stock_bars[-1]["close"]
    ma = moving_average_map(stock_bars)
    if stop_mode == "最近回檔低點":
        lows = [bar["low"] for bar in stock_bars[-20:] if bar.get("low") is not None]
        return min(lows) if lows else None
    if stop_mode == "固定 7%":
        return close * 0.93
    return ma.get("ma20")


def calculate_position_size(config, entry_price, stop_price, industry_exposure=0):
    if not entry_price or not stop_price or stop_price >= entry_price:
        return {
            "valid": False,
            "message": "停損價高於或等於進場價，無法計算部位。",
        }
    risk_amount = config.capital * config.risk_per_trade
    risk_per_share = abs(entry_price - stop_price)
    position_value = risk_amount / risk_per_share * entry_price
    max_position_value = config.capital * config.max_single_position
    capped_position_value = min(position_value, max_position_value)
    shares = math.floor(capped_position_value / entry_price)
    lots = shares // 1000
    odd_lot = shares % 1000
    max_loss = shares * risk_per_share
    first_take_profit = entry_price * 1.08
    second_take_profit = entry_price * 1.15
    return {
        "valid": True,
        "riskAmount": round(risk_amount, 0),
        "positionValue": round(capped_position_value, 0),
        "rawPositionValue": round(position_value, 0),
        "shares": shares,
        "lots": lots,
        "oddLot": odd_lot,
        "stopPrice": round(stop_price, 2),
        "maxLoss": round(max_loss, 0),
        "riskRewardTo8Pct": round((first_take_profit - entry_price) / risk_per_share, 2),
        "firstTakeProfit": round(first_take_profit, 2),
        "secondTakeProfit": round(second_take_profit, 2),
        "exceedsSingleLimit": position_value > max_position_value,
        "exceedsIndustryLimit": industry_exposure + capped_position_value > config.capital * config.max_industry_exposure,
    }


def stock_score(stock, industry_score, market_return20):
    bars = stock["bars"]
    ma = moving_average_map(bars)
    close = bars[-1]["close"]
    ret20 = calculate_return(bars, 20)
    rs20 = calculate_relative_strength(ret20, market_return20 or 0) or 0
    volume_ratio = calculate_volume_ratio(bars) or 0
    trend_points = 30 if close > ma["ma20"] > ma["ma60"] > ma["ma120"] else 0
    rs_points = min(25, max(0, (rs20 + 0.10) / 0.20 * 25))
    industry_points = min(20, max(0, industry_score / 100 * 20))
    structure_points = min(15, max(0, volume_ratio / 1.8 * 15))
    distance = (close - ma["ma20"]) / ma["ma20"] if ma["ma20"] else 99
    risk_points = 10 if 0 <= distance <= 0.15 else max(0, 10 - abs(distance) * 50)
    return round(trend_points + rs_points + industry_points + structure_points + risk_points, 1)


def analyze_stock(stock, industry_score, market_bars):
    bars = stock.get("bars", [])
    if len(bars) < 120:
        return None
    market_return20 = calculate_return(market_bars, 20) or 0
    ma = moving_average_map(bars)
    close = bars[-1]["close"]
    ret5 = calculate_return(bars, 5) or 0
    ret20 = calculate_return(bars, 20) or 0
    ret60 = calculate_return(bars, 60) or 0
    rs20 = calculate_relative_strength(ret20, market_return20) or 0
    volume_ratio = calculate_volume_ratio(bars) or 0
    exit_warning = detect_exit_warning(bars)
    overheated = detect_overheated(bars)
    breakout = detect_breakout(bars)
    pullback = detect_pullback_rebound(bars)
    trend_ok = bool(ma["ma20"] and ma["ma60"] and ma["ma120"] and close > ma["ma20"] > ma["ma60"] > ma["ma120"])

    if trend_ok and breakout:
        signal = "強勢突破"
        action = "可分批"
        bucket = "breakout"
    elif trend_ok and pullback:
        signal = "回檔轉強"
        action = "可觀察"
        bucket = "pullback"
    elif overheated:
        signal = "過熱觀察"
        action = "過熱勿追"
        bucket = "overheated"
    elif exit_warning:
        signal = exit_warning["signal"]
        action = exit_warning["action"]
        bucket = "exit"
    else:
        return None

    stop_prices = {
        "MA20": calculate_stop_price(bars, "MA20"),
        "最近回檔低點": calculate_stop_price(bars, "最近回檔低點"),
        "固定 7%": calculate_stop_price(bars, "固定 7%"),
    }
    default_config = RiskConfig()
    default_stop = stop_prices.get(default_config.stop_mode)
    position = calculate_position_size(default_config, close, default_stop)

    return {
        "code": stock["code"],
        "name": stock["name"],
        "industry": stock.get("industry") or "未分類",
        "baseIndustry": stock.get("baseIndustry") or stock.get("industry") or "未分類",
        "close": round(close, 2),
        "price": round(close, 2),
        "date": bars[-1]["date"],
        "return5": round(ret5 * 100, 2),
        "return20": round(ret20 * 100, 2),
        "return60": round(ret60 * 100, 2),
        "relativeStrength20": round(rs20 * 100, 2),
        "volumeRatio": round(volume_ratio, 2),
        "signal": signal,
        "bucket": bucket,
        "score": stock_score(stock, industry_score, market_return20),
        "action": action,
        "ma5": round(ma["ma5"], 2) if ma["ma5"] else None,
        "ma10": round(ma["ma10"], 2) if ma["ma10"] else None,
        "ma20": round(ma["ma20"], 2) if ma["ma20"] else None,
        "ma60": round(ma["ma60"], 2) if ma["ma60"] else None,
        "ma120": round(ma["ma120"], 2) if ma["ma120"] else None,
        "stopPrices": {key: round(value, 2) if value else None for key, value in stop_prices.items()},
        "positionSizing": position,
    }


def stock_snapshot(stock, industry_score, market_bars):
    bars = stock.get("bars", [])
    if len(bars) < 120:
        return None
    market_return20 = calculate_return(market_bars, 20) or 0
    ma = moving_average_map(bars)
    close = bars[-1]["close"]
    ret5 = calculate_return(bars, 5) or 0
    ret20 = calculate_return(bars, 20) or 0
    ret60 = calculate_return(bars, 60) or 0
    rs20 = calculate_relative_strength(ret20, market_return20) or 0
    volume_ratio = calculate_volume_ratio(bars) or 0
    signal = analyze_stock(stock, industry_score, market_bars)
    return {
        "code": stock["code"],
        "name": stock["name"],
        "industry": stock.get("industry") or "未分類",
        "baseIndustry": stock.get("baseIndustry") or stock.get("industry") or "未分類",
        "close": round(close, 2),
        "date": bars[-1]["date"],
        "return5": round(ret5 * 100, 2),
        "return20": round(ret20 * 100, 2),
        "return60": round(ret60 * 100, 2),
        "relativeStrength20": round(rs20 * 100, 2),
        "volumeRatio": round(volume_ratio, 2),
        "score": stock_score(stock, industry_score, market_return20),
        "signal": signal["signal"] if signal else "趨勢觀察",
        "action": signal["action"] if signal else "觀察",
        "ma20": round(ma["ma20"], 2) if ma["ma20"] else None,
        "ma60": round(ma["ma60"], 2) if ma["ma60"] else None,
    }


def rank_us_industries(us_industries, market_bars):
    market_return20 = calculate_return(market_bars, 20) or 0
    rows = []
    for item in us_industries:
        bars = item.get("bars", [])
        if len(bars) < 60:
            continue
        ret5 = calculate_return(bars, 5)
        ret20 = calculate_return(bars, 20)
        ret60 = calculate_return(bars, 60)
        vol_ratio = calculate_volume_ratio(bars)
        rs20 = calculate_relative_strength(ret20, market_return20)
        if ret5 is None or ret20 is None or ret60 is None:
            continue
        rows.append({
            "symbol": item["symbol"],
            "name": item["name"],
            "mappedIndustries": item.get("mappedIndustries", []),
            "return5": ret5,
            "return20": ret20,
            "return60": ret60,
            "volumeRatio": vol_ratio or 0,
            "relativeStrength20": rs20 or 0,
        })

    ret5_values = [row["return5"] for row in rows]
    ret20_values = [row["return20"] for row in rows]
    ret60_values = [row["return60"] for row in rows]
    rs_values = [row["relativeStrength20"] for row in rows]
    volume_values = [row["volumeRatio"] for row in rows]

    for row in rows:
        score = (
            rank_score(ret5_values, row["return5"]) * 0.20
            + rank_score(ret20_values, row["return20"]) * 0.35
            + rank_score(ret60_values, row["return60"]) * 0.20
            + rank_score(rs_values, row["relativeStrength20"]) * 0.20
            + rank_score(volume_values, row["volumeRatio"]) * 0.05
        )
        row["industryScore"] = round(score, 1)
        row["status"] = industry_label(score)

    rows.sort(key=lambda item: item["industryScore"], reverse=True)
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    return rows


def build_us_taiwan_matches(us_industries, stocks, market_bars, limit_per_us_industry=8):
    matches = []
    for us_row in us_industries[:6]:
        mapped = set(us_row.get("mappedIndustries", []))
        if not mapped:
            continue
        snapshots = []
        for stock in stocks:
            if stock.get("industry") not in mapped and stock.get("baseIndustry") not in mapped:
                continue
            snapshot = stock_snapshot(stock, us_row["industryScore"], market_bars)
            if snapshot:
                snapshots.append(snapshot)
        snapshots.sort(key=lambda item: (item["score"], item["return20"], item["volumeRatio"]), reverse=True)
        matches.append({
            "symbol": us_row["symbol"],
            "name": us_row["name"],
            "rank": us_row["rank"],
            "industryScore": us_row["industryScore"],
            "status": us_row["status"],
            "return5": round(us_row["return5"] * 100, 2),
            "return20": round(us_row["return20"] * 100, 2),
            "return60": round(us_row["return60"] * 100, 2),
            "relativeStrength20": round(us_row["relativeStrength20"] * 100, 2),
            "volumeRatio": round(us_row["volumeRatio"], 2),
            "mappedIndustries": sorted(mapped),
            "stocks": snapshots[:limit_per_us_industry],
        })
    return matches


def generate_taiwan_holy_grail_report(data, config=None):
    config = config or RiskConfig()
    market_bars = data.get("marketBars", [])
    industries = data.get("industries", {})
    loaded_stocks = data.get("stocks", [])
    us_industries = data.get("usIndustries", [])
    market = get_market_regime(market_bars)
    industry_rankings = rank_industries(industries, market_bars)
    strong_industries = {row["industry"]: row for row in industry_rankings if row["industryScore"] >= 65}
    us_taiwan_matches = build_us_taiwan_matches(us_industries, loaded_stocks, market_bars)

    candidates = {"breakout": [], "pullback": [], "overheated": [], "exit": []}
    for industry_name, stocks in industries.items():
        industry = strong_industries.get(industry_name)
        if not industry:
            continue
        for stock in stocks:
            analyzed = analyze_stock(stock, industry["industryScore"], market_bars)
            if analyzed:
                candidates[analyzed["bucket"]].append(analyzed)

    for rows in candidates.values():
        rows.sort(key=lambda item: item["score"], reverse=True)

    return {
        "title": "台股聖杯雷達",
        "subtitle": "大盤決定倉位，產業決定方向，個股決定進場，風控決定能不能活下來。",
        "disclaimer": "本工具僅供研究與教育用途，不構成投資建議。",
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "targetDate": data.get("targetDate"),
        "dataSource": data.get("dataSource", "twstock + yfinance"),
        "market": market,
        "riskConfig": {
            "capital": config.capital,
            "riskPerTrade": config.risk_per_trade,
            "maxSinglePosition": config.max_single_position,
            "maxIndustryExposure": config.max_industry_exposure,
            "stopMode": config.stop_mode,
        },
        "industries": industry_rankings[:10],
        "usIndustries": us_industries[:10],
        "usTaiwanMatches": us_taiwan_matches,
        "candidates": candidates,
        "rerun": {
            "githubActions": "Actions > Daily Stock Scan > Run workflow，可選只重跑台股聖杯與美股產業資料。",
            "localCommand": "python rerun_holy_grail.py --date YYYY-MM-DD",
        },
        "rules": [
            "我只在大盤多頭時積極做多。",
            "我只買強勢產業。",
            "我只買產業裡的強勢股。",
            "我不追漲停。",
            "我等突破確認或回檔轉強。",
            "我每筆最多虧 0.5%～1%。",
            "我跌破 20 日線就出場。",
            "我不凹單、不攤平、不亂融資。",
            "我讓強勢股自己告訴我何時該賣。",
        ],
    }


def fetch_history(symbol, start_dt, end_dt):
    try:
        df = yf.Ticker(symbol).history(start=start_dt, end=end_dt, auto_adjust=False)
        return dataframe_to_bars(df)
    except Exception:
        return []


def fetch_us_industries(start_dt, end_dt, target_date):
    market_bars = fetch_history("SPY", start_dt, end_dt)
    if not market_bars:
        market_bars = fetch_history("^GSPC", start_dt, end_dt)
    market_bars = [bar for bar in market_bars if bar["date"] <= target_date]
    rows = []
    for spec in US_INDUSTRY_ETFS:
        bars = fetch_history(spec["symbol"], start_dt, end_dt)
        bars = [bar for bar in bars if bar["date"] <= target_date]
        if len(bars) < 60:
            continue
        rows.append({**spec, "bars": bars})
    return rank_us_industries(rows, market_bars)


def get_taiwan_stock_universe(max_per_industry=8):
    groups = {}
    for code, info in twstock.codes.items():
        if not (len(code) == 4 and code.isdigit()):
            continue
        if info.type != "股票" or not info.group:
            continue
        if info.market not in {"上市", "上櫃"}:
            continue
        suffix = ".TW" if info.market == "上市" else ".TWO"
        fine_industry = classify_taiwan_industry(code, info.name, info.group)
        groups.setdefault(fine_industry, []).append({
            "code": f"{code}{suffix}",
            "name": info.name,
            "industry": fine_industry,
            "baseIndustry": info.group,
        })
    stocks = []
    for industry, rows in groups.items():
        stocks.extend(rows[:max_per_industry])
    return stocks


def generate_holy_grail_report_from_yfinance(target_date=None, max_per_industry=8, max_workers=24):
    target_dt = datetime.strptime(target_date, "%Y-%m-%d") if target_date else datetime.now()
    start_dt = target_dt - timedelta(days=520)
    end_dt = target_dt + timedelta(days=7)
    market_bars = fetch_history("^TWII", start_dt, end_dt)
    if not market_bars:
        market_bars = fetch_history("0050.TW", start_dt, end_dt)
    target_date_text = target_dt.strftime("%Y-%m-%d")
    market_bars = [bar for bar in market_bars if bar["date"] <= target_date_text]

    universe = get_taiwan_stock_universe(max_per_industry=max_per_industry)
    industries = {}
    loaded_stocks = []

    def load_stock(stock):
        bars = fetch_history(stock["code"], start_dt, end_dt)
        bars = [bar for bar in bars if bar["date"] <= target_date_text]
        if len(bars) < 120:
            return None
        return {**stock, "bars": bars}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(load_stock, stock) for stock in universe]
        for future in as_completed(futures):
            stock = future.result()
            if not stock:
                continue
            loaded_stocks.append(stock)
            industries.setdefault(stock["industry"], []).append(stock)

    us_industries = fetch_us_industries(start_dt, end_dt, target_date_text)

    return generate_taiwan_holy_grail_report({
        "marketBars": market_bars,
        "industries": industries,
        "stocks": loaded_stocks,
        "usIndustries": us_industries,
        "targetDate": target_date_text,
        "dataSource": "twstock universe + yfinance history",
    })


calculateSMA = calculate_sma
calculateReturn = calculate_return
calculateVolumeRatio = calculate_volume_ratio
calculateRelativeStrength = calculate_relative_strength
getMarketRegime = get_market_regime
rankIndustries = rank_industries
analyzeStock = analyze_stock
detectBreakout = detect_breakout
detectPullbackRebound = detect_pullback_rebound
detectOverheated = detect_overheated
detectExitWarning = detect_exit_warning
calculateStopPrice = calculate_stop_price
calculatePositionSize = calculate_position_size
generateTaiwanHolyGrailReport = generate_taiwan_holy_grail_report
classifyTaiwanIndustry = classify_taiwan_industry
