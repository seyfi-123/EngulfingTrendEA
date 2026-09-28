# EngulfingTrend Bot v6.2.0
# MARKET DATA: Twelve Data
#
# v6.1.0 -> v6.2.0 (FAQAT quyidagilar o'zgardi, savdo logikasi
# (SL/TP/risk/history filter/engulfing) TEGILMAGAN):
#
#  FIX A) WebSocket "subscribe-status" va xato xabarlari endi
#         jimgina tashlanmaydi -> log qilinadi va agar obuna
#         muvaffaqiyatsiz bo'lsa Telegram'ga ogohlantirish ketadi.
#         (Bu "jim qolish" muammosining eng ehtimolli sababi edi:
#          socket ochiq, lekin obuna rad etilgan bo'lishi mumkin.)
#
#  FIX B) health_check() endi faqat ogohlantirmaydi -> uzoq jim
#         qolgan stream'ni MAJBURIY qayta ulaydi (reconnect_event),
#         va bitta hodisa uchun faqat BITTA marta ogohlantiradi
#         (avvalgidek har daqiqada takrorlanmaydi), tiklanganda
#         "tiklandi" deb alohida xabar beradi.
#
#  FIX C) close_signal() endi HAR yopilishda (TP_A qisman yopilish,
#         BE, Trailing, SL) to'liq Telegram xabar + yangi grafik
#         yuboradi: sabab, R, Gross/Comm/Net, balance, va o'sha
#         signal ochilgandagi tarixiy (history) kontekst.
#
# Boshqa hech narsa o'zgartirilmadi.

import os
import io
import sys
import math
import time
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import aiohttp
import websockets
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from dotenv import load_dotenv
from telegram import Bot, InputFile
from telegram.constants import ParseMode


# ============================================================
# ENV
# ============================================================

load_dotenv()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Railway: attach a Volume, mount it at /data and set DATA_DIR=/data
DATA_DIR = os.getenv("DATA_DIR", "./data").strip()


def _f(name, default):
    return float(os.getenv(name, str(default)))


def _i(name, default):
    return int(os.getenv(name, str(default)))


def _list(name, default):
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    "SYMBOLS": _list("SYMBOLS", "SPY,WTI/USD,XAU/USD,USD/JPY,EUR/USD"),
    "INTERVALS": _list("INTERVALS", "1m,5m,15m,1h"),
    "INTERVAL": os.getenv("INTERVAL", "1m"),
    "BALANCE": _f("BALANCE", 1000),
    "RISK_PCT": _f("RISK_PCT", 0.02),
    "PART_RISK_RATIO": _f("PART_RISK_RATIO", 0.50),
    "LOT_MIN": _f("LOT_MIN", 0.001),
    "LOT_MAX": _f("LOT_MAX", 5.0),
    "MIN_RISK_USD": _f("MIN_RISK_USD", 1.0),
    "MAX_OPEN_POS": _i("MAX_OPEN_POS", 10),
    "PRELOAD_CANDLES": _i("PRELOAD_CANDLES", 300),
    "HISTORY_MONTHS": _i("HISTORY_MONTHS", 6),
    "HISTORY_LOOKBACK_CANDLES": _i("HISTORY_LOOKBACK_CANDLES", 10),
    "SL_BUF": _f("SL_BUF", 10),
    "REALTIME_ENTRY": os.getenv("REALTIME_ENTRY", "true").lower() == "true",
    "CONFIRM_SECONDS": _f("CONFIRM_SECONDS", 2.5),
    "CONFIRM_TICKS": _i("CONFIRM_TICKS", 2),
    "A_TP_R": _f("A_TP_R", 2),
    "BE_AT_R": _f("BE_AT_R", 2),
    "TRAIL_STEP_R": _f("TRAIL_STEP_R", 2),
    "MAX_TRAIL_R": _f("MAX_TRAIL_R", 10),
    "COMM_RATE": _f("COMM_RATE", 0),
    "MIN_SIMILAR": _i("MIN_SIMILAR", 25),
    "SIMILAR_GOOD_PCT": _f("SIMILAR_GOOD_PCT", 0.30),
    "BODY_TOL": _f("BODY_TOL", 0.35),
    "RANGE_TOL": _f("RANGE_TOL", 0.35),
    "WICK_TOL": _f("WICK_TOL", 0.35),
    "STRUCT_TOL": _f("STRUCT_TOL", 0.50),
    "DIST_TOL": _f("DIST_TOL", 0.50),
    "TIME_BUCKET_MINUTES": _i("TIME_BUCKET_MINUTES", 60),
    "TIME_MIN_SAMPLES": _i("TIME_MIN_SAMPLES", 20),
    "TIME_MIN_GOOD_PCT": _f("TIME_MIN_GOOD_PCT", 0.20),
    "MAX_CONSECUTIVE_LOSSES": _i("MAX_CONSECUTIVE_LOSSES", 10),
    "REPORT_HOUR": _i("REPORT_HOUR", 18),
    "DIAGNOSTICS_HOUR": _i("DIAGNOSTICS_HOUR", 9),
    "SOCKET_TIMEOUT_MIN": _i("SOCKET_TIMEOUT_MIN", 30),
    "MAX_DAILY_LOSS_PCT": _f("MAX_DAILY_LOSS_PCT", 0.15),
    "CHART_CANDLES": _i("CHART_CANDLES", 100),
    "HISTORY_LIMIT": _i("HISTORY_LIMIT", 1000),
    "MAX_HISTORY_CANDIDATES": _i("MAX_HISTORY_CANDIDATES", 5000),
    # Twelve Data free plan = 8 credits/min. 6 leaves a safety margin.
    "TD_REQ_PER_MIN": _i("TD_REQ_PER_MIN", 6),

    # ---- FIX B: reconnect / alert tuning ----
    # Stream shu qadar sekund jim tursa -> majburiy reconnect.
    "STREAM_STALE_SEC": _i("STREAM_STALE_SEC", 90),
}


# ============================================================
# LOGGING  (stdout -> Railway shows INFO as info, not error)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

log = logging.getLogger("EngulfingTrend")


# ============================================================
# TWELVE DATA
# ============================================================

TD_BASE = "https://api.twelvedata.com"
TD_WS = "wss://ws.twelvedata.com/v1/quotes/price"


def td_interval(interval):
    mapping = {
        "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
        "45m": "45min", "1h": "1h", "2h": "2h", "4h": "4h",
        "8h": "8h", "1d": "1day",
    }
    return mapping.get(interval, interval)


def interval_seconds(interval):
    mapping = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800, "45m": 2700,
        "1h": 3600, "2h": 7200, "4h": 14400, "8h": 28800, "1d": 86400,
    }
    return mapping.get(interval, 60)


def normalize_symbol(symbol):
    symbol = symbol.strip().upper()
    aliases = {
        "GOLD": "XAU/USD", "XAUUSD": "XAU/USD",
        "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD",
        "USDJPY": "USD/JPY",
        "AUDUSD": "AUD/USD", "NZDUSD": "NZD/USD",
        "USDCAD": "USD/CAD", "USDCHF": "USD/CHF",
        "SP500": "SPX", "S&P500": "SPX", "S&P 500": "SPX",
    }
    return aliases.get(symbol, symbol)


def td_date(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class DailyLimitError(Exception):
    pass


class TwelveData:

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = None
        self.request_times = []
        self.lock = asyncio.Lock()
        self.max_rpm = max(1, CONFIG["TD_REQ_PER_MIN"])
        self.daily_exhausted = False

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60)
            )

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def _throttle(self):
        """Sliding-window limiter. Called before EVERY http attempt."""
        async with self.lock:
            while True:
                now = time.monotonic()
                self.request_times = [
                    t for t in self.request_times if now - t < 61
                ]
                if len(self.request_times) < self.max_rpm:
                    self.request_times.append(now)
                    return
                wait = 61 - (now - self.request_times[0]) + 0.5
                log.info(
                    "Rate limiter: waiting %.1f sec...", max(wait, 1)
                )
                await asyncio.sleep(max(wait, 1))

    async def _backoff(self, attempt, retry_after, why):
        try:
            base = float(retry_after) if retry_after else 0
        except Exception:
            base = 0
        wait = max(base, 65) + attempt * 20
        log.warning(
            "Twelve Data %s. Waiting %.0f sec (attempt %d/6)",
            why, wait, attempt + 1
        )
        await asyncio.sleep(wait)
        # a full minute has passed -> fresh window
        async with self.lock:
            self.request_times = []

    async def request(self, endpoint, params):
        await self.start()

        params = dict(params)
        params["apikey"] = self.api_key
        url = f"{TD_BASE}/{endpoint}"

        for attempt in range(6):

            await self._throttle()

            try:
                async with self.session.get(url, params=params) as r:
                    text = await r.text()
                    status = r.status
                    retry_after = r.headers.get("Retry-After")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= 5:
                    raise
                wait = 5 * (attempt + 1)
                log.warning(
                    "Twelve Data connection error: %s. Retry in %d sec",
                    e, wait
                )
                await asyncio.sleep(wait)
                continue

            if status == 429:
                await self._backoff(attempt, retry_after, "HTTP 429")
                continue

            if status != 200:
                raise RuntimeError(
                    f"Twelve Data HTTP {status}: {text[:500]}"
                )

            try:
                data = json.loads(text)
            except Exception:
                raise RuntimeError(
                    f"Invalid Twelve Data response: {text[:500]}"
                )

            if isinstance(data, dict) and data.get("status") == "error":

                message = str(data.get("message", "Twelve Data API error"))
                low = message.lower()
                code = data.get("code")

                if "no data" in low:
                    return {"values": []}

                if (
                    "current day" in low
                    or "per day" in low
                    or "daily" in low
                    or "for the day" in low
                ):
                    raise DailyLimitError(message)

                if (
                    code == 429
                    or "limit" in low
                    or "credits" in low
                    or "too many" in low
                ):
                    await self._backoff(attempt, None, f"limit: {message[:120]}")
                    continue

                raise RuntimeError(message)

            return data

        raise RuntimeError("Twelve Data request failed after 6 attempts")

    async def time_series(
        self, symbol, interval, outputsize=5000,
        start_date=None, end_date=None,
    ):
        params = {
            "symbol": normalize_symbol(symbol),
            "interval": td_interval(interval),
            "outputsize": outputsize,
            "format": "JSON",
            "timezone": "UTC",
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return await self.request("time_series", params)

    async def price_stream(self, symbols):
        """
        FIX A: endi faqat yield qilmaydi -> ulanish/obuna holatini ham
        log qiladi, shunda market_stream() qaysi obuna muvaffaqiyatsiz
        bo'lganini bila oladi.
        """

        if not symbols:
            return

        url = f"{TD_WS}?apikey={self.api_key}"

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=2**20,
        ) as ws:

            subscribe_symbols = ",".join(
                normalize_symbol(x) for x in symbols
            )

            await ws.send(json.dumps({
                "action": "subscribe",
                "params": {"symbols": subscribe_symbols},
            }))

            log.info(
                "Twelve Data WebSocket subscribe so'rovi yuborildi: %s",
                subscribe_symbols
            )

            async for raw in ws:
                yield raw


# ============================================================
# TELEGRAM
# ============================================================

class TG:

    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token=token) if token else None

    async def send(self, text):
        if not self.bot or not self.chat_id:
            return
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.error("Telegram send error: %s", e)

    async def send_chart(self, image_bytes, caption=""):
        if not self.bot or not self.chat_id:
            return
        try:
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=InputFile(io.BytesIO(image_bytes), filename="chart.png"),
                caption=caption[:1024],
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            log.error("Telegram chart error: %s", e)


TG_CLIENT = TG(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)


# ============================================================
# CANDLE HELPERS
# ============================================================

def candle_range(c):
    return max(float(c["high"]) - float(c["low"]), 1e-12)


def body_size(c):
    return abs(float(c["close"]) - float(c["open"]))


def body_ratio(c):
    return body_size(c) / candle_range(c)


def upper_wick_ratio(c):
    h = float(c["high"])
    o = float(c["open"])
    cl = float(c["close"])
    return max(0.0, h - max(o, cl)) / candle_range(c)


def lower_wick_ratio(c):
    l = float(c["low"])
    o = float(c["open"])
    cl = float(c["close"])
    return max(0.0, min(o, cl) - l) / candle_range(c)


def close_position(c):
    return (float(c["close"]) - float(c["low"])) / candle_range(c)


def signed_body(c):
    r = candle_range(c)
    if r <= 0:
        return 0.0
    return (float(c["close"]) - float(c["open"])) / r


# ============================================================
# STRUCTURE
# ============================================================

def structure_features(candles):

    if len(candles) < 3:
        return {
            "trend": 0.0, "hh": 0.0, "hl": 0.0,
            "lh": 0.0, "ll": 0.0, "range_pos": 0.5,
        }

    recent = candles[-10:]

    highs = [float(x["high"]) for x in recent]
    lows = [float(x["low"]) for x in recent]

    trend = float(recent[-1]["close"]) - float(recent[0]["open"])

    avg_range = sum(candle_range(x) for x in recent) / max(len(recent), 1)

    trend_norm = trend / max(avg_range * len(recent), 1e-12)

    hh = hl = lh = ll = 0

    for i in range(1, len(recent)):
        if highs[i] > highs[i - 1]:
            hh += 1
        if lows[i] > lows[i - 1]:
            hl += 1
        if highs[i] < highs[i - 1]:
            lh += 1
        if lows[i] < lows[i - 1]:
            ll += 1

    last = recent[-1]

    range_pos = (float(last["close"]) - min(lows)) / max(
        max(highs) - min(lows), 1e-12
    )

    n = max(len(recent) - 1, 1)

    return {
        "trend": trend_norm,
        "hh": hh / n,
        "hl": hl / n,
        "lh": lh / n,
        "ll": ll / n,
        "range_pos": range_pos,
    }


# ============================================================
# ENGULFING
# ============================================================

def detect_engulfing(candles, idx):

    if idx < 1:
        return None

    prev = candles[idx - 1]
    cur = candles[idx]

    po = float(prev["open"])
    pc = float(prev["close"])
    co = float(cur["open"])
    cc = float(cur["close"])

    if pc < po and cc > co and co <= pc and cc >= po:
        return "BUY"

    if pc > po and cc < co and co >= pc and cc <= po:
        return "SELL"

    return None


# ============================================================
# FEATURES
# ============================================================

def make_features(candles, idx, signal):

    start = max(0, idx - CONFIG["HISTORY_LOOKBACK_CANDLES"] + 1)

    sample = candles[start:idx + 1]

    if not sample:
        return None

    current = candles[idx]

    ranges = [candle_range(x) for x in sample]
    avg_range = sum(ranges) / max(len(ranges), 1)

    sf = structure_features(sample)

    return {
        "signal": signal,
        "body": body_ratio(current),
        "range": candle_range(current) / max(avg_range, 1e-12),
        "upper_wick": upper_wick_ratio(current),
        "lower_wick": lower_wick_ratio(current),
        "close_position": close_position(current),
        "signed_body": signed_body(current),
        "trend": sf["trend"],
        "hh": sf["hh"],
        "hl": sf["hl"],
        "lh": sf["lh"],
        "ll": sf["ll"],
        "range_pos": sf["range_pos"],
        "entry": float(current["close"]),
        "high": float(current["high"]),
        "low": float(current["low"]),
    }


# ============================================================
# DISTANCE
# ============================================================

def normalized_distance(a, b):
    if a is None or b is None:
        return 999.0
    return abs(float(a) - float(b))


def feature_distance(a, b):

    if not a or not b:
        return 999.0

    dist = 0.0

    dist += min(normalized_distance(a["body"], b["body"])
                / max(CONFIG["BODY_TOL"], 1e-12), 5)

    dist += min(normalized_distance(a["range"], b["range"])
                / max(CONFIG["RANGE_TOL"], 1e-12), 5)

    dist += min(normalized_distance(a["upper_wick"], b["upper_wick"])
                / max(CONFIG["WICK_TOL"], 1e-12), 5)

    dist += min(normalized_distance(a["lower_wick"], b["lower_wick"])
                / max(CONFIG["WICK_TOL"], 1e-12), 5)

    for key in ["trend", "hh", "hl", "lh", "ll", "range_pos"]:
        dist += min(
            normalized_distance(a.get(key, 0), b.get(key, 0))
            / max(CONFIG["STRUCT_TOL"], 1e-12),
            5
        )

    return dist


# ============================================================
# HISTORICAL TRADE EVALUATION
# ============================================================

def evaluate_historical_trade(candles, idx, signal):

    if idx >= len(candles) - 1:
        return None

    setup = candles[idx]
    entry = float(setup["close"])

    if signal == "BUY":

        sl = float(setup["low"])
        risk = entry - sl

        if risk <= 0:
            return None

        tp = entry + risk * CONFIG["A_TP_R"]

        for j in range(idx + 1, len(candles)):
            c = candles[j]
            h = float(c["high"])
            l = float(c["low"])

            if l <= sl:
                return -1.0
            if h >= tp:
                return CONFIG["A_TP_R"]

    else:

        sl = float(setup["high"])
        risk = sl - entry

        if risk <= 0:
            return None

        tp = entry - risk * CONFIG["A_TP_R"]

        for j in range(idx + 1, len(candles)):
            c = candles[j]
            h = float(c["high"])
            l = float(c["low"])

            if h >= sl:
                return -1.0
            if l <= tp:
                return CONFIG["A_TP_R"]

    return 0.0


# ============================================================
# HISTORICAL DATABASE
# ============================================================

class HistoricalDB:

    def __init__(self):
        self.rows = {}

    def key(self, symbol, interval):
        return (normalize_symbol(symbol), interval)

    def build_time_stats(self, symbol, interval):

        rows = self.rows.get(self.key(symbol, interval), [])

        buckets = {}

        for row in rows:
            bucket = row.get("time_bucket")
            if bucket is None:
                continue
            buckets.setdefault(bucket, []).append(row)

        for row in rows:
            bucket = row.get("time_bucket")
            if bucket not in buckets:
                continue

            group = buckets[bucket]

            results = [
                x["result"] for x in group if x.get("result") is not None
            ]

            if len(results) < CONFIG["TIME_MIN_SAMPLES"]:
                row["time_good_pct"] = None
            else:
                good = sum(1 for x in results if x > 0)
                row["time_good_pct"] = good / len(results)

    def add(self, symbol, interval, features, result, timestamp):

        if features is None:
            return

        dt = pd.to_datetime(timestamp, utc=True, errors="coerce")

        if pd.isna(dt):
            return

        minutes = dt.hour * 60 + dt.minute
        bucket_size = CONFIG["TIME_BUCKET_MINUTES"]
        bucket = (minutes // bucket_size) * bucket_size

        row = {
            "features": features,
            "result": result,
            "time_bucket": bucket,
            "timestamp": dt,
        }

        self.rows.setdefault(self.key(symbol, interval), []).append(row)

    def similar(self, symbol, interval, features):

        rows = self.rows.get(self.key(symbol, interval), [])

        candidates = []

        for row in rows:

            if row.get("features") is None:
                continue

            if row["features"].get("signal") != features.get("signal"):
                continue

            d = feature_distance(features, row["features"])

            if d <= 10:
                candidates.append((d, row))

        candidates.sort(key=lambda x: x[0])

        return [
            x[1] for x in candidates[:CONFIG["MAX_HISTORY_CANDIDATES"]]
        ]

    def evaluate(self, symbol, interval, features):

        candidates = self.similar(symbol, interval, features)

        if len(candidates) < CONFIG["MIN_SIMILAR"]:
            return {
                "ok": False,
                "reason": f"similar={len(candidates)} < {CONFIG['MIN_SIMILAR']}",
                "count": len(candidates),
                "good_pct": 0.0,
            }

        results = [
            x["result"] for x in candidates if x.get("result") is not None
        ]

        if not results:
            return {
                "ok": False,
                "reason": "no historical results",
                "count": 0,
                "good_pct": 0.0,
            }

        good = sum(1 for x in results if x > 0)
        good_pct = good / len(results)

        current_time = features.get("timestamp")

        if current_time is not None:

            dt_current = pd.to_datetime(current_time, utc=True)

            bucket = dt_current.hour * 60 + dt_current.minute
            bucket = (
                bucket // CONFIG["TIME_BUCKET_MINUTES"]
            ) * CONFIG["TIME_BUCKET_MINUTES"]

            time_rows = [
                x for x in self.rows.get(self.key(symbol, interval), [])
                if x.get("time_bucket") == bucket
            ]

            if len(time_rows) >= CONFIG["TIME_MIN_SAMPLES"]:

                time_good = sum(
                    1 for x in time_rows if x.get("result", 0) > 0
                )

                time_good_pct = time_good / len(time_rows)

                if time_good_pct < CONFIG["TIME_MIN_GOOD_PCT"]:
                    return {
                        "ok": False,
                        "reason": f"time filter {time_good_pct:.2%}",
                        "count": len(results),
                        "good_pct": good_pct,
                    }

        ok = good_pct >= CONFIG["SIMILAR_GOOD_PCT"]

        return {
            "ok": ok,
            "reason": "passed" if ok else f"good={good_pct:.2%}",
            "count": len(results),
            "good_pct": good_pct,
        }


# ============================================================
# DATA CONVERSION
# ============================================================

def td_values_to_candles(values):

    candles = []

    for row in values or []:
        try:
            dt = pd.to_datetime(row["datetime"], utc=True)

            candles.append({
                "timestamp": dt.to_pydatetime(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0) or 0),
                "closed": True,
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["timestamp"])

    return candles


# ============================================================
# DISK CACHE
# ============================================================

def cache_path(symbol, interval):
    safe = normalize_symbol(symbol).replace("/", "_").replace(" ", "")
    return os.path.join(DATA_DIR, f"{safe}_{interval}.csv")


def load_cache(symbol, interval):

    path = cache_path(symbol, interval)

    if not os.path.exists(path):
        return []

    try:
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        candles = []

        for ts, o, h, l, c, v in zip(
            df["timestamp"], df["open"], df["high"],
            df["low"], df["close"], df["volume"]
        ):
            candles.append({
                "timestamp": ts.to_pydatetime(),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v),
                "closed": True,
            })

        candles.sort(key=lambda x: x["timestamp"])

        log.info(
            "Disk cache loaded %s %s: %d candles",
            symbol, interval, len(candles)
        )

        return candles

    except Exception as e:
        log.error("Cache read error %s %s: %s", symbol, interval, e)
        return []


def save_cache(symbol, interval, candles):

    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        path = cache_path(symbol, interval)
        tmp = path + ".tmp"

        df = pd.DataFrame(
            [
                {
                    "timestamp": c["timestamp"].isoformat(),
                    "open": c["open"],
                    "high": c["high"],
                    "low": c["low"],
                    "close": c["close"],
                    "volume": c.get("volume", 0.0),
                }
                for c in candles
            ]
        )

        df.to_csv(tmp, index=False)
        os.replace(tmp, path)

        log.info(
            "Disk cache saved %s %s: %d candles",
            symbol, interval, len(candles)
        )

    except Exception as e:
        log.error("Cache write error %s %s: %s", symbol, interval, e)


# ============================================================
# HISTORY DOWNLOAD (incremental)
# ============================================================

HISTORY_CACHE = {}


async def fetch_range(client, symbol, interval, start_dt, end_dt):
    """Pages backwards from end_dt to start_dt. Returns (candles, ok)."""

    rows = []
    ok = True
    current_end = end_dt
    step = timedelta(seconds=interval_seconds(interval))

    for _ in range(400):

        if current_end <= start_dt:
            break

        try:
            data = await client.time_series(
                symbol=symbol,
                interval=interval,
                outputsize=5000,
                start_date=td_date(start_dt),
                end_date=td_date(current_end),
            )

        except DailyLimitError as e:
            client.daily_exhausted = True
            log.error("Twelve Data DAILY credit limit reached: %s", e)
            ok = False
            break

        except Exception as e:
            log.error("History error %s %s: %s", symbol, interval, e)
            ok = False
            break

        values = data.get("values", [])

        if not values:
            break

        batch = td_values_to_candles(values)

        if not batch:
            break

        rows.extend(batch)

        log.info(
            "History %s %s: +%d (total %d)",
            symbol, interval, len(batch), len(rows)
        )

        earliest = batch[0]["timestamp"]

        # fewer than a full page -> range exhausted, no extra request
        if earliest <= start_dt or len(values) < 5000:
            break

        new_end = earliest - step

        if new_end >= current_end:
            break

        current_end = new_end

    return rows, ok


async def download_history(client, symbol, interval):

    symbol = normalize_symbol(symbol)

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=30 * CONFIG["HISTORY_MONTHS"])
    step_s = interval_seconds(interval)

    cached = [
        c for c in load_cache(symbol, interval)
        if c["timestamp"] >= start_dt
    ]

    new = []
    ok = True

    if not client.daily_exhausted:

        if cached:

            last_ts = cached[-1]["timestamp"]

            if (now - last_ts).total_seconds() > step_s * 2:
                tail, ok1 = await fetch_range(
                    client, symbol, interval, last_ts, now
                )
                new += tail
                ok = ok and ok1

            first_ts = cached[0]["timestamp"]

            if ok and (first_ts - start_dt) > timedelta(days=7):
                head, ok2 = await fetch_range(
                    client, symbol, interval, start_dt, first_ts
                )
                new += head
                ok = ok and ok2

        else:
            new, ok = await fetch_range(
                client, symbol, interval, start_dt, now
            )

    # Partial download on top of an existing cache would leave a gap
    if not ok and cached:
        new = []

    unique = {}

    for c in cached:
        unique[c["timestamp"]] = c

    for c in new:
        unique[c["timestamp"]] = c

    cutoff = now - timedelta(seconds=step_s)

    candles = [
        c for c in unique.values()
        if start_dt <= c["timestamp"] <= cutoff
    ]

    candles.sort(key=lambda x: x["timestamp"])

    if candles and (new or not cached):
        save_cache(symbol, interval, candles)

    return candles


# ============================================================
# BUILD HISTORY DB
# ============================================================

async def build_history_db(client, db, symbol, interval):

    candles = await download_history(client, symbol, interval)

    if len(candles) < 30:
        log.warning(
            "Not enough history %s %s: %d", symbol, interval, len(candles)
        )
        return candles

    added = 0

    for idx in range(
        CONFIG["HISTORY_LOOKBACK_CANDLES"], len(candles) - 1
    ):

        signal = detect_engulfing(candles, idx)

        if not signal:
            continue

        features = make_features(candles, idx, signal)

        if not features:
            continue

        features["timestamp"] = candles[idx]["timestamp"]

        result = evaluate_historical_trade(candles, idx, signal)

        if result is None:
            continue

        db.add(symbol, interval, features, result, candles[idx]["timestamp"])

        added += 1

    db.build_time_stats(symbol, interval)

    log.info(
        "History DB %s %s: candles=%d setups=%d",
        symbol, interval, len(candles), added
    )

    HISTORY_CACHE[(normalize_symbol(symbol), interval)] = candles[
        -CONFIG["PRELOAD_CANDLES"]:
    ]

    return candles


# ============================================================
# CHART
# ============================================================

def make_chart(
    candles, symbol, interval,
    entry=None, exit_price=None, signal=None,
    entry_time=None, exit_time=None,
):
    """
    FIX C: endi entry_time/exit_time berilsa, ochilish va yopilish
    nuqtalarini candle ustida ANIQ belgilaydi (o'q/marker bilan),
    shunchaki gorizontal chiziq emas.
    """

    if not candles:
        return None

    data = candles[-CONFIG["CHART_CANDLES"]:]

    index_by_ts = {c["timestamp"]: i for i, c in enumerate(data)}

    fig, ax = plt.subplots(figsize=(12, 6))

    width = 0.6

    for i, c in enumerate(data):

        o = float(c["open"])
        h = float(c["high"])
        l = float(c["low"])
        cl = float(c["close"])

        color = "#10b981" if cl >= o else "#ef4444"

        ax.plot([i, i], [l, h], linewidth=1, color=color)

        if cl >= o:
            bottom = o
            height = cl - o
        else:
            bottom = cl
            height = o - cl

        rect = plt.Rectangle(
            (i - width / 2, bottom), width, max(height, 1e-12),
            fill=True, facecolor=color, edgecolor=color, alpha=0.85,
        )

        ax.add_patch(rect)

    span = max(
        max(float(c["high"]) for c in data)
        - min(float(c["low"]) for c in data),
        1e-9,
    )

    def find_idx(ts):
        if ts is None:
            return len(data) - 1
        if ts in index_by_ts:
            return index_by_ts[ts]
        # eng yaqin candle
        best_i, best_d = None, None
        for i, c in enumerate(data):
            d = abs((c["timestamp"] - ts).total_seconds())
            if best_d is None or d < best_d:
                best_i, best_d = i, d
        return best_i if best_i is not None else len(data) - 1

    if entry is not None:
        i = find_idx(entry_time)
        buy = signal == "BUY"
        ax.scatter(
            i, entry, marker="^" if buy else "v",
            color="#10b981" if buy else "#ef4444",
            s=160, zorder=6, edgecolors="white", linewidths=0.7,
        )
        ax.annotate(
            "OPEN", xy=(i, entry),
            xytext=(i, entry - span * 0.03 if buy else entry + span * 0.03),
            ha="center", fontsize=8, fontweight="bold",
            color="#10b981" if buy else "#ef4444",
        )

    if exit_price is not None:
        i = find_idx(exit_time)
        ax.scatter(
            i, exit_price, marker="X",
            color="#facc15", s=140, zorder=7,
            edgecolors="white", linewidths=0.7,
        )
        ax.annotate(
            "EXIT", xy=(i, exit_price),
            xytext=(i, exit_price + span * 0.03),
            ha="center", fontsize=8, fontweight="bold", color="#facc15",
        )

    ax.set_title(f"{symbol} | {interval} | {signal or ''}")
    ax.grid(True, alpha=0.2)

    buf = io.BytesIO()

    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)

    buf.seek(0)

    return buf.getvalue()


# ============================================================
# ENGINE
# ============================================================

class Engine:

    def __init__(self, symbol, interval, history_db):

        self.symbol = normalize_symbol(symbol)
        self.interval = interval
        self.db = history_db

        self.balance = CONFIG["BALANCE"]
        self.start_balance = self.balance

        self.positions = []
        self.candles = []
        self.trades = []

        self.stats = {"wins": 0, "losses": 0, "signals": 0, "blocked": 0}

        self.total_comm = 0.0
        self.gross_pnl = 0.0
        self.consecutive_losses = 0
        self.self_blocked = False

        self.day_start_balance = self.balance
        self.day = datetime.now(timezone.utc).date()

        self.last_candle_time = time.time()
        self.pending = {}
        self.last_report = None

    def reset_day(self):

        today = datetime.now(timezone.utc).date()

        if today != self.day:
            self.day = today
            self.day_start_balance = self.balance
            self.consecutive_losses = 0
            self.self_blocked = False

    def daily_loss_limit_hit(self):

        loss = self.day_start_balance - self.balance
        limit = self.day_start_balance * CONFIG["MAX_DAILY_LOSS_PCT"]

        return loss >= limit

    def can_trade(self):

        self.reset_day()

        if self.self_blocked:
            return False

        if self.daily_loss_limit_hit():
            return False

        if len(self.positions) >= CONFIG["MAX_OPEN_POS"]:
            return False

        return True

    def calc_lot(self, entry, sl, part_risk_ratio=1.0):

        risk_money = self.balance * CONFIG["RISK_PCT"] * part_risk_ratio

        sl_dist = abs(entry - sl)

        if sl_dist <= 0:
            return 0.0

        lot = risk_money / sl_dist

        lot = max(CONFIG["LOT_MIN"], min(lot, CONFIG["LOT_MAX"]))

        return lot

    def open_local(self, signal, entry, sl, history_result=None,
                   opened_at=None):

        risk = (entry - sl) if signal == "BUY" else (sl - entry)

        if risk <= 0:
            return None

        lot_a = self.calc_lot(entry, sl, CONFIG["PART_RISK_RATIO"])
        lot_b = self.calc_lot(entry, sl, CONFIG["PART_RISK_RATIO"])

        if lot_a <= 0:
            return None

        tp_a = (
            entry + risk * CONFIG["A_TP_R"]
            if signal == "BUY"
            else entry - risk * CONFIG["A_TP_R"]
        )

        p = {
            "id": f"{self.symbol}-{self.interval}-{int(time.time() * 1000)}",
            "symbol": self.symbol,
            "interval": self.interval,
            "signal": signal,
            "entry": entry,
            "sl": sl,
            "initial_sl": sl,
            "risk": risk,
            "lot": lot_a + lot_b,
            "lot_a": lot_a,
            "lot_b": lot_b,
            "tp_a": tp_a,
            "tp_b": None,
            "a_closed": False,
            "b_sl": sl,
            "commission": 0.0,
            "gross": 0.0,
            "opened": opened_at or datetime.now(timezone.utc),
            "trail_r": 0.0,
            # FIX C: yopilishda ko'rsatish uchun ochilish konteksti
            "history_result": history_result or {},
        }

        open_comm = p["lot"] * p["entry"] * CONFIG["COMM_RATE"]

        p["commission"] += open_comm
        self.total_comm += open_comm

        self.positions.append(p)

        return p

    # --------------------------------------------------------
    # FIX C: to'liq Telegram YOPILISH xabari + grafik
    # --------------------------------------------------------

    async def _send_close_report(self, p, exit_price, reason,
                                 net_pnl, gross, fraction, r_multiple,
                                 exit_time):

        emoji = "✅" if net_pnl > 0.01 else ("❌" if net_pnl < -0.01 else "⚪")

        part_label = (
            "A (qisman TP)" if reason == "TP_A"
            else ("B (qolgan qism)" if fraction < 0.999 or p["a_closed"]
                  else "TO'LIQ")
        )

        reason_map = {
            "TP_A": "🎯 TP A ga yetdi (qisman yopildi, B davom etadi)",
            "SL_BE_TRAIL": (
                "🛡 Stop / Breakeven / Trailing bosildi"
                if p.get("trail_r", 0) > 0 or p.get("b_sl") == p["entry"]
                else "🛑 Stop-Loss bosildi"
            ),
        }

        reason_text = reason_map.get(reason, reason)

        hist = p.get("history_result") or {}

        txt = (
            f"{emoji} <b>YOPILDI [{part_label}] {p['symbol']} "
            f"[{p['interval']}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 Sabab: {reason_text}\n"
            f"🎯 R: <b>{r_multiple:+.2f}R</b>\n"
            f"💵 Entry: {p['entry']:.6f}\n"
            f"🚪 Exit: {exit_price:.6f}\n"
            f"💵 Gross: ${gross:+.2f}\n"
            f"🔻 Comm: -${p['commission']:.2f}\n"
            f"💰 Net (shu yopilish): <b>${net_pnl:+.2f}</b>\n"
            f"📈 Balance: <b>${self.balance:.2f}</b>\n"
            f"🔥 Ketma-ket zarar: {self.consecutive_losses}"
        )

        if hist:
            txt += (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"📚 Ochilishdagi tarix: similar "
                f"<b>{hist.get('count', 0)}</b>, good "
                f"<b>{hist.get('good_pct', 0):.1%}</b>"
            )

        if self.self_blocked:
            txt += (
                f"\n🛑 BLOCK: {CONFIG['MAX_CONSECUTIVE_LOSSES']} "
                f"ketma-ket zarardan keyin savdo to'xtatildi"
            )

        await TG_CLIENT.send(txt)

        chart = make_chart(
            self.candles, p["symbol"], p["interval"],
            entry=p["entry"], exit_price=exit_price, signal=p["signal"],
            entry_time=p.get("opened"), exit_time=exit_time,
        )

        if chart:
            await TG_CLIENT.send_chart(
                chart,
                caption=(
                    f"{p['symbol']} {p['interval']} · {p['signal']} · "
                    f"{reason_text} · {r_multiple:+.2f}R"
                ),
            )

    def close_signal(self, p, exit_price, reason, fraction=1.0):

        if p not in self.positions:
            return

        lot = p["lot"] * fraction

        if p["signal"] == "BUY":
            gross = (exit_price - p["entry"]) * lot
        else:
            gross = (p["entry"] - exit_price) * lot

        close_comm = lot * exit_price * CONFIG["COMM_RATE"]

        p["commission"] += close_comm
        self.total_comm += close_comm

        net_pnl = gross - close_comm

        self.balance += net_pnl
        self.gross_pnl += gross
        p["gross"] += gross

        exit_time = datetime.now(timezone.utc)

        risk_usd = p["risk"] * lot
        r_multiple = net_pnl / risk_usd if risk_usd > 0 else 0.0

        self.trades.append({
            "time": exit_time,
            "symbol": self.symbol,
            "interval": self.interval,
            "signal": p["signal"],
            "entry": p["entry"],
            "exit": exit_price,
            "pnl": net_pnl,
            "reason": reason,
        })

        if net_pnl > 0:
            self.stats["wins"] += 1
            self.consecutive_losses = 0

        elif net_pnl < 0:
            self.stats["losses"] += 1
            self.consecutive_losses += 1

        newly_blocked = False

        if self.consecutive_losses >= CONFIG["MAX_CONSECUTIVE_LOSSES"]:
            if not self.self_blocked:
                newly_blocked = True
            self.self_blocked = True

        # FIX C: har yopilishda to'liq hisobot + grafik yuborish
        asyncio.create_task(
            self._send_close_report(
                p, exit_price, reason, net_pnl, gross,
                fraction, r_multiple, exit_time,
            )
        )

        if newly_blocked:
            asyncio.create_task(
                TG_CLIENT.send(
                    f"🛑 <b>SELF BLOCK</b>\n"
                    f"{self.symbol} {self.interval}\n"
                    f"{CONFIG['MAX_CONSECUTIVE_LOSSES']} consecutive losses."
                )
            )

        # (o'zgarmagan) faqat to'liq yopilishda positions'dan olib
        # tashlanadi, shunda TP_A B qismini o'chirib qo'ymaydi.
        if fraction >= 0.999:
            try:
                self.positions.remove(p)
            except ValueError:
                pass

        return net_pnl

    def manage_local(self, p, price):

        if p not in self.positions:
            return

        signal = p["signal"]
        entry = p["entry"]
        risk = p["risk"]

        if risk <= 0:
            return

        is_buy = signal == "BUY"

        r_now = (price - entry) / risk if is_buy else (entry - price) / risk

        tp_hit = price >= p["tp_a"] if is_buy else price <= p["tp_a"]

        # ---- TP A (partial close) ----
        if not p["a_closed"] and tp_hit:

            old_lot = p["lot"]

            if old_lot > 0:

                self.close_signal(
                    p, p["tp_a"], "TP_A",
                    fraction=p["lot_a"] / old_lot
                )

                p["a_closed"] = True

                if p in self.positions:
                    p["lot"] -= p["lot_a"]
                    p["lot_a"] = 0
                    p["b_sl"] = entry

        # ---- Break-even ----
        if p in self.positions and r_now >= CONFIG["BE_AT_R"]:
            p["b_sl"] = (
                max(p["b_sl"], entry) if is_buy else min(p["b_sl"], entry)
            )

        # ---- Trailing ----
        if p in self.positions and r_now >= CONFIG["TRAIL_STEP_R"]:

            trail_r = min(
                math.floor(r_now / CONFIG["TRAIL_STEP_R"])
                * CONFIG["TRAIL_STEP_R"],
                CONFIG["MAX_TRAIL_R"]
            )

            if trail_r > p["trail_r"]:

                p["trail_r"] = trail_r

                shift = risk * max(0, trail_r - CONFIG["TRAIL_STEP_R"])

                if is_buy:
                    p["b_sl"] = max(p["b_sl"], entry + shift)
                else:
                    p["b_sl"] = min(p["b_sl"], entry - shift)

        # ---- Stop ----
        if p in self.positions:

            stop_hit = price <= p["b_sl"] if is_buy else price >= p["b_sl"]

            if stop_hit:
                self.close_signal(p, p["b_sl"], "SL_BE_TRAIL")

    def manage_positions(self, price):
        for p in list(self.positions):
            self.manage_local(p, price)

    def history_check(self, features):
        return self.db.evaluate(self.symbol, self.interval, features)

    async def open_signal(self, signal, candle):

        if not self.can_trade():
            self.stats["blocked"] += 1
            return

        self.stats["signals"] += 1

        entry = float(candle["close"])

        sl = float(candle["low"]) if signal == "BUY" else float(candle["high"])

        buf = abs(entry) * CONFIG["SL_BUF"] / 100000

        if signal == "BUY":
            sl -= buf
        else:
            sl += buf

        features = make_features(
            self.candles, len(self.candles) - 1, signal
        )

        if not features:
            return

        features["timestamp"] = candle["timestamp"]

        result = self.history_check(features)

        if not result["ok"]:
            log.info(
                "FILTER %s %s %s: %s",
                self.symbol, self.interval, signal, result["reason"]
            )
            return

        p = self.open_local(
            signal, entry, sl,
            history_result=result,
            opened_at=candle["timestamp"],
        )

        if not p:
            return

        msg = (
            f"🟢 <b>{signal}</b>\n"
            f"<b>{self.symbol}</b> | {self.interval}\n"
            f"Entry: {entry:.6f}\n"
            f"SL: {sl:.6f}\n"
            f"TP A: {p['tp_a']:.6f}\n"
            f"Similar: {result['count']}\n"
            f"Good: {result['good_pct']:.2%}\n"
            f"Balance: {self.balance:.2f}"
        )

        await TG_CLIENT.send(msg)

        chart = make_chart(
            self.candles, self.symbol, self.interval,
            entry=entry, signal=signal, entry_time=candle["timestamp"],
        )

        if chart:
            await TG_CLIENT.send_chart(
                chart,
                caption=f"{self.symbol} {self.interval} {signal} · OPEN"
            )

    async def handle_closed(self, candle):

        self.last_candle_time = time.time()

        if not self.candles:
            self.candles.append(candle)
        else:
            last = self.candles[-1]
            if last["timestamp"] == candle["timestamp"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)

        self.manage_positions(float(candle["close"]))

        if len(self.candles) < 2:
            return

        idx = len(self.candles) - 1

        signal = detect_engulfing(self.candles, idx)

        if signal:
            await self.open_signal(signal, candle)

        max_keep = max(CONFIG["HISTORY_LIMIT"], CONFIG["PRELOAD_CANDLES"])

        if len(self.candles) > max_keep:
            self.candles = self.candles[-max_keep:]

    async def handle_realtime(self, candle):

        self.last_candle_time = time.time()

        price = float(candle["close"])

        self.manage_positions(price)

        if not CONFIG["REALTIME_ENTRY"]:
            return

        if len(self.candles) < 2:
            return

        test_candles = list(self.candles)

        if test_candles and test_candles[-1]["timestamp"] == candle["timestamp"]:
            test_candles[-1] = candle
        else:
            test_candles.append(candle)

        if len(test_candles) < 2:
            return

        idx = len(test_candles) - 1

        signal = detect_engulfing(test_candles, idx)

        if not signal:
            return

        key = signal
        now = time.time()

        pending = self.pending.get(key)

        if pending is None or pending["candle_time"] != candle["timestamp"]:
            self.pending[key] = {
                "started": now,
                "ticks": 1,
                "candle_time": candle["timestamp"],
            }
            return

        pending["ticks"] += 1

        elapsed = now - pending["started"]

        if (
            pending["ticks"] >= CONFIG["CONFIRM_TICKS"]
            and elapsed >= CONFIG["CONFIRM_SECONDS"]
        ):

            candle_key = f"{candle['timestamp']}-{signal}"

            if self.pending.get("executed") == candle_key:
                return

            self.pending["executed"] = candle_key

            old = self.candles
            self.candles = test_candles

            try:
                await self.open_signal(signal, candle)
            finally:
                self.candles = old


# ============================================================
# GLOBAL ENGINES
# ============================================================

ENGINES = {}


# ============================================================
# PRELOAD CURRENT
# ============================================================

async def preload_current(client, engine):

    cache_key = (normalize_symbol(engine.symbol), engine.interval)

    cached = HISTORY_CACHE.get(cache_key)

    if cached:
        engine.candles = [dict(c) for c in cached]
        engine.last_candle_time = time.time()
        log.info(
            "Preload from history cache %s %s: %d candles",
            engine.symbol, engine.interval, len(engine.candles)
        )
        return

    if client.daily_exhausted:
        log.warning(
            "Preload skipped %s %s (daily API limit)",
            engine.symbol, engine.interval
        )
        return

    try:
        data = await client.time_series(
            symbol=engine.symbol,
            interval=engine.interval,
            outputsize=CONFIG["PRELOAD_CANDLES"] + 2,
        )

        candles = td_values_to_candles(data.get("values", []))

        if candles:
            engine.candles = candles[-CONFIG["PRELOAD_CANDLES"]:]
            engine.last_candle_time = time.time()
            log.info(
                "Preload API %s %s: %d candles",
                engine.symbol, engine.interval, len(engine.candles)
            )

    except Exception as e:
        log.error(
            "Preload error %s %s: %s",
            engine.symbol, engine.interval, e
        )


# ============================================================
# LIVE CANDLE BUILDER
# ============================================================

LIVE_CANDLES = {}


def candle_bucket(timestamp, interval):

    sec = interval_seconds(interval)
    ts = int(timestamp.timestamp())
    bucket = (ts // sec) * sec

    return datetime.fromtimestamp(bucket, tz=timezone.utc)


async def process_price_tick(symbol, price, timestamp=None):

    symbol = normalize_symbol(symbol)

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    price = float(price)

    for key, engine in list(ENGINES.items()):

        if engine.symbol != symbol:
            continue

        interval = engine.interval

        bucket = candle_bucket(timestamp, interval)

        live_key = (symbol, interval)

        current = LIVE_CANDLES.get(live_key)

        if current is None:

            current = {
                "timestamp": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
                "closed": False,
            }

            LIVE_CANDLES[live_key] = current

            await engine.handle_realtime(current)
            continue

        if current["timestamp"] == bucket:

            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price

            await engine.handle_realtime(current)
            continue

        previous = dict(current)
        previous["closed"] = True

        await engine.handle_closed(previous)

        new_candle = {
            "timestamp": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
            "closed": False,
        }

        LIVE_CANDLES[live_key] = new_candle

        await engine.handle_realtime(new_candle)


# ============================================================
# PRICE WEBSOCKET
# ============================================================

# FIX B: health_check shu Event orqali market_stream'ga
# "majburiy qayta ulan" deb signal beradi.
RECONNECT_EVENT = asyncio.Event()

# FIX B: bitta uzilish hodisasi uchun faqat bitta marta ogohlantirish
_ALERT_SENT = False
_LAST_TICK_TS = {"t": time.time()}


async def market_stream(client):

    global _ALERT_SENT

    symbols = [normalize_symbol(x) for x in CONFIG["SYMBOLS"]]

    reconnect_delay = 3

    while True:

        RECONNECT_EVENT.clear()

        try:

            log.info("Twelve Data narx oqimiga ulanmoqda...")

            stream_gen = client.price_stream(symbols)

            recv_task = None

            async def _next_message():
                return await stream_gen.__anext__()

            while True:

                if recv_task is None:
                    recv_task = asyncio.create_task(_next_message())

                wait_event = asyncio.create_task(RECONNECT_EVENT.wait())

                done, pending = await asyncio.wait(
                    {recv_task, wait_event},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if wait_event in done:
                    # FIX B: health_check majburiy reconnect so'radi
                    recv_task.cancel()
                    for t in pending:
                        t.cancel()
                    log.warning(
                        "market_stream: health_check reconnect so'radi, "
                        "socket qayta ochilmoqda"
                    )
                    raise ConnectionError("forced_reconnect")

                wait_event.cancel()

                if recv_task in done:

                    raw = recv_task.result()
                    recv_task = None

                    reconnect_delay = 3
                    _LAST_TICK_TS["t"] = time.time()

                    if _ALERT_SENT:
                        _ALERT_SENT = False
                        await TG_CLIENT.send(
                            "✅ <b>Market data oqimi tiklandi</b>"
                        )

                    try:

                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")

                        msg = json.loads(raw)

                        if not isinstance(msg, dict):
                            continue

                        event = msg.get("event")

                        # ---------------------------------------
                        # FIX A: subscribe-status/heartbeat/boshqa
                        # xato hodisalari endi log qilinadi va
                        # muvaffaqiyatsiz obuna Telegram'ga
                        # ogohlantiriladi (jimgina tashlanmaydi).
                        # ---------------------------------------

                        if event == "subscribe-status":

                            status = msg.get("status")
                            success = msg.get("success") or []
                            fails = msg.get("fails") or []

                            log.info(
                                "TD subscribe-status: status=%s "
                                "success=%s fails=%s",
                                status, success, fails
                            )

                            if fails:
                                await TG_CLIENT.send(
                                    "⚠️ <b>Twelve Data OBUNA "
                                    "MUVAFFAQIYATSIZ</b>\n"
                                    f"Fails: {fails}\n"
                                    "Symbol nomi yoki tarif rejasini "
                                    "tekshiring."
                                )

                            if status and status != "ok":
                                await TG_CLIENT.send(
                                    f"⚠️ <b>Twelve Data subscribe-status: "
                                    f"{status}</b>\n<code>{str(msg)[:500]}"
                                    f"</code>"
                                )

                            continue

                        if event == "heartbeat":
                            continue

                        if event in ("error",) or msg.get("code") not in (
                            None, 200,
                        ):
                            log.error(
                                "TD stream error message: %s", str(msg)[:500]
                            )
                            await TG_CLIENT.send(
                                "⚠️ <b>Twelve Data stream xato "
                                f"xabari</b>\n<code>{str(msg)[:500]}</code>"
                            )
                            continue

                        symbol = msg.get("symbol") or msg.get("code")

                        price = (
                            msg.get("price")
                            or msg.get("close")
                            or msg.get("value")
                        )

                        if not symbol or price is None:
                            # tanimagan xabar turi -> yo'qotmaslik uchun log
                            log.debug("TD stream noma'lum xabar: %s", msg)
                            continue

                        ts_value = msg.get("timestamp") or msg.get("time")

                        ts = None

                        if ts_value is not None:
                            try:
                                if isinstance(ts_value, (int, float)):
                                    ts = datetime.fromtimestamp(
                                        float(ts_value), tz=timezone.utc
                                    )
                                else:
                                    ts = pd.to_datetime(
                                        ts_value, utc=True
                                    ).to_pydatetime()
                            except Exception:
                                ts = None

                        await process_price_tick(symbol, float(price), ts)

                    except Exception as e:
                        log.error("Price message error: %s", e)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            log.error("Market stream disconnected: %s", e)

            await asyncio.sleep(reconnect_delay)

            reconnect_delay = min(reconnect_delay * 2, 60)


# ============================================================
# REPORT
# ============================================================

async def daily_report():

    lines = ["📊 <b>DAILY REPORT</b>", ""]

    total_balance = 0.0
    total_pnl = 0.0
    total_trades = 0

    for engine in ENGINES.values():

        pnl = engine.balance - CONFIG["BALANCE"]

        total_balance += engine.balance
        total_pnl += pnl
        total_trades += len(engine.trades)

        lines.append(
            f"<b>{engine.symbol} {engine.interval}</b> | "
            f"Balance {engine.balance:.2f} | "
            f"PnL {pnl:.2f} | "
            f"W {engine.stats['wins']} | "
            f"L {engine.stats['losses']}"
        )

    lines.extend([
        "",
        f"Total balance: {total_balance:.2f}",
        f"Total PnL: {total_pnl:.2f}",
        f"Trades: {total_trades}",
    ])

    await TG_CLIENT.send("\n".join(lines))


# ============================================================
# HEALTH CHECK  (FIX B)
# ============================================================

async def health_check():
    """
    Avvalgi versiya faqat har 60 sekundda ogohlantirar edi va
    hech qachon o'zi tuzatmasdi. Endi:
      - Global oxirgi tick vaqtiga qaraydi (barcha symbol/TF umumiy
        bitta socket orqali kelgani uchun bitta global soat yetarli).
      - STREAM_STALE_SEC dan uzoqroq jim bo'lsa: BIR marta ogohlantiradi
        va RECONNECT_EVENT orqali market_stream'ni majburan qayta
        ulanishga majbur qiladi.
      - Tiklangach market_stream o'zi "tiklandi" deb xabar beradi.
    """

    global _ALERT_SENT

    while True:

        try:
            now = time.time()
            silent = now - _LAST_TICK_TS["t"]

            if silent > CONFIG["STREAM_STALE_SEC"] and not _ALERT_SENT:

                _ALERT_SENT = True

                await TG_CLIENT.send(
                    "⚠️ <b>MARKET DATA JIM</b>\n"
                    f"{silent / 60:.1f} daqiqadan beri tick yo'q.\n"
                    "🔄 Socket majburiy qayta ulanmoqda..."
                )

                RECONNECT_EVENT.set()

            # ham per-engine eski loglash (diagnostika uchun foydali)
            for key, engine in ENGINES.items():
                age = now - engine.last_candle_time
                if age > CONFIG["SOCKET_TIMEOUT_MIN"] * 60:
                    log.warning(
                        "%s %s: %.1f min candle kelmagan",
                        engine.symbol, engine.interval, age / 60
                    )

        except Exception as e:
            log.error("Health error: %s", e)

        await asyncio.sleep(20)


# ============================================================
# DIAGNOSTICS
# ============================================================

async def diagnostics_loop():

    last_day = None

    while True:

        try:

            now = datetime.now(timezone.utc)

            if (
                now.hour == CONFIG["DIAGNOSTICS_HOUR"]
                and now.minute < 5
                and last_day != now.date()
            ):

                last_day = now.date()

                lines = ["🔎 <b>BOT DIAGNOSTICS</b>", ""]

                for engine in ENGINES.values():
                    lines.append(
                        f"{engine.symbol} {engine.interval}: "
                        f"candles={len(engine.candles)}, "
                        f"positions={len(engine.positions)}, "
                        f"signals={engine.stats['signals']}, "
                        f"blocked={engine.self_blocked}"
                    )

                await TG_CLIENT.send("\n".join(lines))

        except Exception as e:
            log.error("Diagnostics error: %s", e)

        await asyncio.sleep(30)


# ============================================================
# REPORT LOOP
# ============================================================

async def report_loop():

    last_day = None

    while True:

        try:

            now = datetime.now(timezone.utc)

            if (
                now.hour == CONFIG["REPORT_HOUR"]
                and now.minute < 5
                and last_day != now.date()
            ):

                last_day = now.date()

                await daily_report()

        except Exception as e:
            log.error("Report loop error: %s", e)

        await asyncio.sleep(30)


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing")

    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN is missing")

    if not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_CHAT_ID is missing")

    log.info("=" * 50)
    log.info("EngulfingTrend Bot v6.2.0")
    log.info("Market data: Twelve Data")
    log.info("Symbols: %s", CONFIG["SYMBOLS"])
    log.info("Intervals: %s", CONFIG["INTERVALS"])
    log.info("Commission: %.8f", CONFIG["COMM_RATE"])
    log.info("Data dir: %s", DATA_DIR)
    log.info("=" * 50)

    os.makedirs(DATA_DIR, exist_ok=True)

    client = TwelveData(TWELVE_DATA_API_KEY)
    db = HistoricalDB()

    _LAST_TICK_TS["t"] = time.time()

    try:

        await client.start()

        symbols = [normalize_symbol(s) for s in CONFIG["SYMBOLS"]]
        intervals = CONFIG["INTERVALS"]
        total = len(symbols) * len(intervals)
        n = 0

        await TG_CLIENT.send(
            "⏳ <b>EngulfingTrend</b>: history loading "
            f"({total} datasets). First run can take a while."
        )

        # ---------------- BUILD HISTORY ----------------

        for symbol in symbols:
            for interval in intervals:

                n += 1

                log.info(
                    "Building history [%d/%d]: %s %s",
                    n, total, symbol, interval
                )

                await build_history_db(client, db, symbol, interval)

        # ---------------- CREATE ENGINES ----------------

        for symbol in symbols:
            for interval in intervals:

                engine = Engine(symbol, interval, db)

                ENGINES[(symbol, interval)] = engine

                await preload_current(client, engine)

        # ---------------- TELEGRAM START ----------------

        await TG_CLIENT.send(
            "🟢 <b>EngulfingTrend Bot v6.2.0 STARTED</b>\n\n"
            "Market: Twelve Data\n"
            f"Symbols: {', '.join(CONFIG['SYMBOLS'])}\n"
            f"Timeframes: {', '.join(CONFIG['INTERVALS'])}\n"
            f"Commission: {CONFIG['COMM_RATE']}\n"
            f"Historical DB: {CONFIG['HISTORY_MONTHS']} months\n"
            f"Stale reconnect: {CONFIG['STREAM_STALE_SEC']}s"
        )

        # ---------------- RUN ----------------

        tasks = [
            asyncio.create_task(market_stream(client)),
            asyncio.create_task(health_check()),
            asyncio.create_task(diagnostics_loop()),
            asyncio.create_task(report_loop()),
        ]

        await asyncio.gather(*tasks)

    finally:

        await client.close()

        log.info("Bot stopped.")


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        log.info("Stopped by user.")

    except Exception as e:
        log.exception("FATAL ERROR: %s", e)