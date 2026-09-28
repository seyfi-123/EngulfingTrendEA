# EngulfingTrend Bot v6.0.0
# MARKET DATA: Twelve Data
# Binance removed.
# Telegram / strategy / historical similarity / time filter / A-B / risk / BE / trailing /
# consecutive-loss block / charts / Railway structure preserved.

import os
import io
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


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    "SYMBOLS": [
        x.strip()
        for x in os.getenv(
            "SYMBOLS",
            "XAU/USD,EUR/USD,GBP/USD,SPX"
        ).split(",")
        if x.strip()
    ],

    "INTERVALS": [
        x.strip()
        for x in os.getenv(
            "INTERVALS",
            "1m,5m,15m,1h"
        ).split(",")
        if x.strip()
    ],

    "INTERVAL": os.getenv("INTERVAL", "1m"),

    "BALANCE": float(os.getenv("BALANCE", "1000")),
    "RISK_PCT": float(os.getenv("RISK_PCT", "0.02")),
    "PART_RISK_RATIO": float(os.getenv("PART_RISK_RATIO", "0.50")),

    "LOT_MIN": float(os.getenv("LOT_MIN", "0.001")),
    "LOT_MAX": float(os.getenv("LOT_MAX", "5.0")),
    "MIN_RISK_USD": float(os.getenv("MIN_RISK_USD", "1.0")),

    "MAX_OPEN_POS": int(os.getenv("MAX_OPEN_POS", "10")),

    "PRELOAD_CANDLES": int(os.getenv("PRELOAD_CANDLES", "300")),
    "HISTORY_MONTHS": int(os.getenv("HISTORY_MONTHS", "6")),
    "HISTORY_LOOKBACK_CANDLES": int(
        os.getenv("HISTORY_LOOKBACK_CANDLES", "10")
    ),

    "SL_BUF": float(os.getenv("SL_BUF", "10")),

    "REALTIME_ENTRY": os.getenv(
        "REALTIME_ENTRY", "true"
    ).lower() == "true",

    "CONFIRM_SECONDS": float(
        os.getenv("CONFIRM_SECONDS", "2.5")
    ),

    "CONFIRM_TICKS": int(
        os.getenv("CONFIRM_TICKS", "2")
    ),

    "A_TP_R": float(os.getenv("A_TP_R", "2")),
    "BE_AT_R": float(os.getenv("BE_AT_R", "2")),
    "TRAIL_STEP_R": float(os.getenv("TRAIL_STEP_R", "2")),
    "MAX_TRAIL_R": float(os.getenv("MAX_TRAIL_R", "10")),

    "COMM_RATE": float(os.getenv("COMM_RATE", "0")),

    "MIN_SIMILAR": int(os.getenv("MIN_SIMILAR", "25")),
    "SIMILAR_GOOD_PCT": float(
        os.getenv("SIMILAR_GOOD_PCT", "0.40")
    ),

    "BODY_TOL": float(os.getenv("BODY_TOL", "0.35")),
    "RANGE_TOL": float(os.getenv("RANGE_TOL", "0.35")),
    "WICK_TOL": float(os.getenv("WICK_TOL", "0.35")),
    "STRUCT_TOL": float(os.getenv("STRUCT_TOL", "0.50")),
    "DIST_TOL": float(os.getenv("DIST_TOL", "0.50")),

    "TIME_BUCKET_MINUTES": int(
        os.getenv("TIME_BUCKET_MINUTES", "60")
    ),
    "TIME_MIN_SAMPLES": int(
        os.getenv("TIME_MIN_SAMPLES", "20")
    ),
    "TIME_MIN_GOOD_PCT": float(
        os.getenv("TIME_MIN_GOOD_PCT", "0.20")
    ),

    "MAX_CONSECUTIVE_LOSSES": int(
        os.getenv("MAX_CONSECUTIVE_LOSSES", "10")
    ),

    "REPORT_HOUR": int(
        os.getenv("REPORT_HOUR", "18")
    ),
    "DIAGNOSTICS_HOUR": int(
        os.getenv("DIAGNOSTICS_HOUR", "9")
    ),

    "SOCKET_TIMEOUT_MIN": int(
        os.getenv("SOCKET_TIMEOUT_MIN", "30")
    ),

    "MAX_DAILY_LOSS_PCT": float(
        os.getenv("MAX_DAILY_LOSS_PCT", "0.15")
    ),

    "CHART_CANDLES": int(
        os.getenv("CHART_CANDLES", "100")
    ),

    "HISTORY_LIMIT": int(
        os.getenv("HISTORY_LIMIT", "1000")
    ),

    "MAX_HISTORY_CANDIDATES": int(
        os.getenv("MAX_HISTORY_CANDIDATES", "5000")
    ),
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("EngulfingTrend")


# ============================================================
# TWELVE DATA
# ============================================================

TD_BASE = "https://api.twelvedata.com"
TD_WS = "wss://ws.twelvedata.com/v1/quotes/price"


def td_interval(interval):
    mapping = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "45m": "45min",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "8h": "8h",
        "1d": "1day",
    }
    return mapping.get(interval, interval)


def interval_seconds(interval):
    mapping = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "45m": 2700,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "8h": 28800,
        "1d": 86400,
    }

    return mapping.get(interval, 60)


def normalize_symbol(symbol):
    symbol = symbol.strip().upper()

    aliases = {
        "GOLD": "XAU/USD",
        "XAUUSD": "XAU/USD",
        "EURUSD": "EUR/USD",
        "GBPUSD": "GBP/USD",
        "USDJPY": "USD/JPY",
        "AUDUSD": "AUD/USD",
        "NZDUSD": "NZD/USD",
        "USDCAD": "USD/CAD",
        "USDCHF": "USD/CHF",
        "SP500": "SPX",
        "S&P500": "SPX",
        "S&P 500": "SPX",
    }

    return aliases.get(symbol, symbol)


class TwelveData:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = None

        # Twelve Data Basic limit:
        # 8 credits/minute.
        # We intentionally use 7/minute as safety margin.
        self.request_times = []
        self.request_lock = asyncio.Lock()

    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60)
            )

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def request(self, endpoint, params):
        await self.start()

        # ----------------------------------------------------
        # RATE LIMIT PROTECTION
        # ----------------------------------------------------

        async with self.request_lock:

            while True:

                now = time.monotonic()

                self.request_times = [
                    t
                    for t in self.request_times
                    if now - t < 60
                ]

                if len(self.request_times) < 7:
                    self.request_times.append(now)
                    break

                wait_time = (
                    60
                    - (now - self.request_times[0])
                    + 0.5
                )

                log.warning(
                    "Twelve Data rate limit protection: "
                    "waiting %.1f seconds...",
                    wait_time
                )

                await asyncio.sleep(
                    max(wait_time, 1)
                )

        params = dict(params)
        params["apikey"] = self.api_key

        url = f"{TD_BASE}/{endpoint}"

        # ----------------------------------------------------
        # RETRY
        # ----------------------------------------------------

        for attempt in range(5):

            try:

                async with self.session.get(
                    url,
                    params=params
                ) as r:

                    text = await r.text()

                    # ------------------------------------------------
                    # HTTP 429
                    # ------------------------------------------------

                    if r.status == 429:

                        retry_after = r.headers.get(
                            "Retry-After"
                        )

                        try:
                            wait = float(
                                retry_after
                            )
                        except Exception:
                            wait = 65

                        log.warning(
                            "Twelve Data HTTP 429. "
                            "Waiting %.1f seconds "
                            "(attempt %d/5)",
                            wait,
                            attempt + 1
                        )

                        await asyncio.sleep(
                            max(wait, 60)
                        )

                        continue

                    # ------------------------------------------------
                    # OTHER HTTP ERRORS
                    # ------------------------------------------------

                    if r.status != 200:
                        raise RuntimeError(
                            f"Twelve Data HTTP "
                            f"{r.status}: {text[:500]}"
                        )

                    # ------------------------------------------------
                    # JSON
                    # ------------------------------------------------

                    try:
                        data = json.loads(text)

                    except Exception:
                        raise RuntimeError(
                            "Invalid Twelve Data "
                            f"response: {text[:500]}"
                        )

                    # ------------------------------------------------
                    # API ERROR
                    # ------------------------------------------------

                    if (
                        isinstance(data, dict)
                        and data.get("status") == "error"
                    ):

                        message = data.get(
                            "message",
                            "Twelve Data API error"
                        )

                        if (
                            "limit" in message.lower()
                            or "credits" in message.lower()
                            or "too many" in message.lower()
                        ):

                            log.warning(
                                "Twelve Data rate limit: %s",
                                message
                            )

                            await asyncio.sleep(65)

                            continue

                        raise RuntimeError(
                            message
                        )

                    return data

            except aiohttp.ClientError as e:

                if attempt >= 4:
                    raise

                wait = 5 * (
                    attempt + 1
                )

                log.warning(
                    "Twelve Data connection error: %s. "
                    "Retrying in %d sec...",
                    e,
                    wait
                )

                await asyncio.sleep(wait)

        raise RuntimeError(
            "Twelve Data request failed "
            "after 5 attempts"
        )

    async def time_series(
        self,
        symbol,
        interval,
        outputsize=5000,
        start_date=None,
        end_date=None,
    ):
        params = {
            "symbol": normalize_symbol(symbol),
            "interval": td_interval(interval),
            "outputsize": outputsize,
            "format": "JSON",
        }

        if start_date:
            params["start_date"] = start_date

        if end_date:
            params["end_date"] = end_date

        return await self.request(
            "time_series",
            params
        )

    async def price_stream(self, symbols):
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
                normalize_symbol(x)
                for x in symbols
            )

            await ws.send(
                json.dumps({
                    "action": "subscribe",
                    "params": {
                        "symbols": subscribe_symbols
                    }
                })
            )

            log.info(
                "Twelve Data WebSocket subscribed: %s",
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
            log.error(
                "Telegram send error: %s",
                e
            )

    async def send_chart(
        self,
        image_bytes,
        caption=""
    ):
        if not self.bot or not self.chat_id:
            return

        try:
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=InputFile(
                    io.BytesIO(image_bytes),
                    filename="chart.png"
                ),
                caption=caption[:1024],
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            log.error(
                "Telegram chart error: %s",
                e
            )


TG_CLIENT = TG(
    TELEGRAM_TOKEN,
    TELEGRAM_CHAT_ID
)


# ============================================================
# CANDLE HELPERS
# ============================================================

def candle_range(c):
    return max(
        float(c["high"])
        - float(c["low"]),
        1e-12
    )


def body_size(c):
    return abs(
        float(c["close"])
        - float(c["open"])
    )


def body_ratio(c):
    return (
        body_size(c)
        / candle_range(c)
    )


def upper_wick_ratio(c):
    h = float(c["high"])
    o = float(c["open"])
    cl = float(c["close"])

    return max(
        0.0,
        h - max(o, cl)
    ) / candle_range(c)


def lower_wick_ratio(c):
    l = float(c["low"])
    o = float(c["open"])
    cl = float(c["close"])

    return max(
        0.0,
        min(o, cl) - l
    ) / candle_range(c)


def close_position(c):
    return (
        (
            float(c["close"])
            - float(c["low"])
        )
        / candle_range(c)
    )


def signed_body(c):
    r = candle_range(c)

    if r <= 0:
        return 0.0

    return (
        (
            float(c["close"])
            - float(c["open"])
        )
        / r
    )


# ============================================================
# STRUCTURE
# ============================================================

def structure_features(candles):
    if len(candles) < 3:
        return {
            "trend": 0.0,
            "hh": 0.0,
            "hl": 0.0,
            "lh": 0.0,
            "ll": 0.0,
            "range_pos": 0.5,
        }

    recent = candles[-10:]

    highs = [
        float(x["high"])
        for x in recent
    ]

    lows = [
        float(x["low"])
        for x in recent
    ]

    trend = (
        float(recent[-1]["close"])
        - float(recent[0]["open"])
    )

    avg_range = sum(
        candle_range(x)
        for x in recent
    ) / max(len(recent), 1)

    trend_norm = (
        trend
        / max(
            avg_range * len(recent),
            1e-12
        )
    )

    hh = 0
    hl = 0
    lh = 0
    ll = 0

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

    range_pos = (
        (
            float(last["close"])
            - min(lows)
        )
        / max(
            max(highs) - min(lows),
            1e-12
        )
    )

    return {
        "trend": trend_norm,
        "hh": hh / max(len(recent) - 1, 1),
        "hl": hl / max(len(recent) - 1, 1),
        "lh": lh / max(len(recent) - 1, 1),
        "ll": ll / max(len(recent) - 1, 1),
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

    if (
        pc < po
        and cc > co
        and co <= pc
        and cc >= po
    ):
        return "BUY"

    if (
        pc > po
        and cc < co
        and co >= pc
        and cc <= po
    ):
        return "SELL"

    return None


# ============================================================
# FEATURES
# ============================================================

def make_features(
    candles,
    idx,
    signal
):
    start = max(
        0,
        idx
        - CONFIG["HISTORY_LOOKBACK_CANDLES"]
        + 1
    )

    sample = candles[
        start:idx + 1
    ]

    if not sample:
        return None

    current = candles[idx]

    ranges = [
        candle_range(x)
        for x in sample
    ]

    avg_range = (
        sum(ranges)
        / max(len(ranges), 1)
    )

    sf = structure_features(
        sample
    )

    return {
        "signal": signal,

        "body": body_ratio(current),

        "range": (
            candle_range(current)
            / max(avg_range, 1e-12)
        ),

        "upper_wick": upper_wick_ratio(
            current
        ),

        "lower_wick": lower_wick_ratio(
            current
        ),

        "close_position": close_position(
            current
        ),

        "signed_body": signed_body(
            current
        ),

        "trend": sf["trend"],
        "hh": sf["hh"],
        "hl": sf["hl"],
        "lh": sf["lh"],
        "ll": sf["ll"],
        "range_pos": sf["range_pos"],

        "entry": float(
            current["close"]
        ),

        "high": float(
            current["high"]
        ),

        "low": float(
            current["low"]
        ),
    }


# ============================================================
# DISTANCE
# ============================================================

def normalized_distance(a, b):
    if a is None or b is None:
        return 999.0

    return abs(
        float(a) - float(b)
    )


def feature_distance(a, b):
    if not a or not b:
        return 999.0

    dist = 0.0

    dist += min(
        normalized_distance(
            a["body"],
            b["body"]
        )
        / max(
            CONFIG["BODY_TOL"],
            1e-12
        ),
        5
    )

    dist += min(
        normalized_distance(
            a["range"],
            b["range"]
        )
        / max(
            CONFIG["RANGE_TOL"],
            1e-12
        ),
        5
    )

    dist += min(
        normalized_distance(
            a["upper_wick"],
            b["upper_wick"]
        )
        / max(
            CONFIG["WICK_TOL"],
            1e-12
        ),
        5
    )

    dist += min(
        normalized_distance(
            a["lower_wick"],
            b["lower_wick"]
        )
        / max(
            CONFIG["WICK_TOL"],
            1e-12
        ),
        5
    )

    structure_keys = [
        "trend",
        "hh",
        "hl",
        "lh",
        "ll",
        "range_pos",
    ]

    for key in structure_keys:

        dist += min(
            normalized_distance(
                a.get(key, 0),
                b.get(key, 0)
            )
            / max(
                CONFIG["STRUCT_TOL"],
                1e-12
            ),
            5
        )

    return dist


# ============================================================
# HISTORICAL TRADE EVALUATION
# ============================================================

def evaluate_historical_trade(
    candles,
    idx,
    signal,
):
    if idx >= len(candles) - 1:
        return None

    setup = candles[idx]

    entry = float(
        setup["close"]
    )

    if signal == "BUY":

        sl = float(
            setup["low"]
        )

        risk = entry - sl

        if risk <= 0:
            return None

        tp = (
            entry
            + risk * CONFIG["A_TP_R"]
        )

        for j in range(
            idx + 1,
            len(candles)
        ):

            c = candles[j]

            h = float(c["high"])
            l = float(c["low"])

            hit_sl = l <= sl
            hit_tp = h >= tp

            if hit_sl and hit_tp:
                return -1.0

            if hit_sl:
                return -1.0

            if hit_tp:
                return CONFIG["A_TP_R"]

    else:

        sl = float(
            setup["high"]
        )

        risk = sl - entry

        if risk <= 0:
            return None

        tp = (
            entry
            - risk * CONFIG["A_TP_R"]
        )

        for j in range(
            idx + 1,
            len(candles)
        ):

            c = candles[j]

            h = float(c["high"])
            l = float(c["low"])

            hit_sl = h >= sl
            hit_tp = l <= tp

            if hit_sl and hit_tp:
                return -1.0

            if hit_sl:
                return -1.0

            if hit_tp:
                return CONFIG["A_TP_R"]

    return 0.0


# ============================================================
# HISTORICAL DATABASE
# ============================================================

class HistoricalDB:

    def __init__(self):
        self.rows = {}

    def key(
        self,
        symbol,
        interval
    ):
        return (
            normalize_symbol(symbol),
            interval
        )

    def build_time_stats(
        self,
        symbol,
        interval
    ):
        key = self.key(
            symbol,
            interval
        )

        rows = self.rows.get(
            key,
            []
        )

        buckets = {}

        for row in rows:

            bucket = row.get(
                "time_bucket"
            )

            if bucket is None:
                continue

            buckets.setdefault(
                bucket,
                []
            ).append(row)

        for row in rows:

            bucket = row.get(
                "time_bucket"
            )

            if bucket not in buckets:
                continue

            group = buckets[bucket]

            results = [
                x["result"]
                for x in group
                if x.get("result") is not None
            ]

            if len(results) < CONFIG[
                "TIME_MIN_SAMPLES"
            ]:

                row[
                    "time_good_pct"
                ] = None

            else:

                good = sum(
                    1
                    for x in results
                    if x > 0
                )

                row[
                    "time_good_pct"
                ] = (
                    good
                    / len(results)
                )

    def add(
        self,
        symbol,
        interval,
        features,
        result,
        timestamp,
    ):
        if features is None:
            return

        dt = pd.to_datetime(
            timestamp,
            utc=True,
            errors="coerce"
        )

        if pd.isna(dt):
            return

        minutes = (
            dt.hour * 60
            + dt.minute
        )

        bucket_size = CONFIG[
            "TIME_BUCKET_MINUTES"
        ]

        bucket = (
            minutes
            // bucket_size
        ) * bucket_size

        row = {
            "features": features,
            "result": result,
            "time_bucket": bucket,
            "timestamp": dt,
        }

        key = self.key(
            symbol,
            interval
        )

        self.rows.setdefault(
            key,
            []
        ).append(row)

    def similar(
        self,
        symbol,
        interval,
        features,
    ):
        key = self.key(
            symbol,
            interval
        )

        rows = self.rows.get(
            key,
            []
        )

        candidates = []

        for row in rows:

            if row.get(
                "features"
            ) is None:
                continue

            if (
                row["features"].get(
                    "signal"
                )
                != features.get(
                    "signal"
                )
            ):
                continue

            d = feature_distance(
                features,
                row["features"]
            )

            if d <= 10:

                candidates.append(
                    (
                        d,
                        row
                    )
                )

        candidates.sort(
            key=lambda x: x[0]
        )

        return [
            x[1]
            for x in candidates[
                :CONFIG[
                    "MAX_HISTORY_CANDIDATES"
                ]
            ]
        ]

    def evaluate(
        self,
        symbol,
        interval,
        features,
    ):
        candidates = self.similar(
            symbol,
            interval,
            features
        )

        if len(candidates) < CONFIG[
            "MIN_SIMILAR"
        ]:

            return {
                "ok": False,
                "reason": (
                    f"similar={len(candidates)} "
                    f"< {CONFIG['MIN_SIMILAR']}"
                ),
                "count": len(candidates),
                "good_pct": 0.0,
            }

        results = [
            x["result"]
            for x in candidates
            if x.get("result") is not None
        ]

        if not results:

            return {
                "ok": False,
                "reason": "no historical results",
                "count": 0,
                "good_pct": 0.0,
            }

        good = sum(
            1
            for x in results
            if x > 0
        )

        good_pct = (
            good
            / len(results)
        )

        current_time = features.get(
            "timestamp"
        )

        if current_time is not None:

            dt_current = pd.to_datetime(
                current_time,
                utc=True
            )

            bucket = (
                dt_current.hour * 60
                + dt_current.minute
            )

            bucket = (
                bucket
                // CONFIG[
                    "TIME_BUCKET_MINUTES"
                ]
            ) * CONFIG[
                "TIME_BUCKET_MINUTES"
            ]

            time_rows = [
                x
                for x in self.rows.get(
                    self.key(
                        symbol,
                        interval
                    ),
                    []
                )
                if x.get(
                    "time_bucket"
                ) == bucket
            ]

            if len(time_rows) >= CONFIG[
                "TIME_MIN_SAMPLES"
            ]:

                time_good = sum(
                    1
                    for x in time_rows
                    if x.get(
                        "result",
                        0
                    ) > 0
                )

                time_good_pct = (
                    time_good
                    / len(time_rows)
                )

                if (
                    time_good_pct
                    < CONFIG[
                        "TIME_MIN_GOOD_PCT"
                    ]
                ):

                    return {
                        "ok": False,
                        "reason": (
                            f"time filter "
                            f"{time_good_pct:.2%}"
                        ),
                        "count": len(results),
                        "good_pct": good_pct,
                    }

        ok = (
            good_pct
            >= CONFIG[
                "SIMILAR_GOOD_PCT"
            ]
        )

        return {
            "ok": ok,
            "reason": (
                "passed"
                if ok
                else f"good={good_pct:.2%}"
            ),
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

            dt = pd.to_datetime(
                row["datetime"],
                utc=True
            )

            candles.append({
                "timestamp": dt.to_pydatetime(),
                "open": float(
                    row["open"]
                ),
                "high": float(
                    row["high"]
                ),
                "low": float(
                    row["low"]
                ),
                "close": float(
                    row["close"]
                ),
                "volume": float(
                    row.get(
                        "volume",
                        0
                    ) or 0
                ),
                "closed": True,
            })

        except Exception:
            continue

    candles.sort(
        key=lambda x: x["timestamp"]
    )

    return candles


# ============================================================
# 6 MONTH HISTORY
# ============================================================

async def download_6m_history(
    client,
    symbol,
    interval,
):
    symbol = normalize_symbol(
        symbol
    )

    end_dt = datetime.now(
        timezone.utc
    )

    start_dt = (
        end_dt
        - timedelta(
            days=30
            * CONFIG[
                "HISTORY_MONTHS"
            ]
        )
    )

    all_rows = []

    current_end = end_dt

    step = timedelta(
        seconds=interval_seconds(
            interval
        )
    )

    max_loops = 200

    for _ in range(max_loops):

        if current_end <= start_dt:
            break

        try:

            data = await client.time_series(
                symbol=symbol,
                interval=interval,
                outputsize=5000,
                start_date=start_dt.isoformat(),
                end_date=current_end.isoformat(),
            )

        except Exception as e:

            log.error(
                "History error %s %s: %s",
                symbol,
                interval,
                e,
            )

            break

        values = data.get(
            "values",
            []
        )

        if not values:
            break

        batch = td_values_to_candles(
            values
        )

        if not batch:
            break

        all_rows.extend(
            batch
        )

        earliest = min(
            x["timestamp"]
            for x in batch
        )

        if earliest <= start_dt:
            break

        new_end = (
            earliest - step
        )

        if new_end >= current_end:
            break

        current_end = new_end

        log.info(
            "History %s %s: %d candles",
            symbol,
            interval,
            len(all_rows)
        )

        # Small delay in addition to
        # the global API limiter.
        await asyncio.sleep(0.5)

    unique = {}

    for c in all_rows:

        unique[
            c["timestamp"].isoformat()
        ] = c

    candles = list(
        unique.values()
    )

    candles.sort(
        key=lambda x: x["timestamp"]
    )

    return candles


# ============================================================
# BUILD HISTORY DB
# ============================================================

async def build_history_db(
    client,
    db,
    symbol,
    interval,
):
    candles = await download_6m_history(
        client,
        symbol,
        interval
    )

    if len(candles) < 30:

        log.warning(
            "Not enough history %s %s: %d",
            symbol,
            interval,
            len(candles)
        )

        return candles

    added = 0

    for idx in range(
        CONFIG[
            "HISTORY_LOOKBACK_CANDLES"
        ],
        len(candles) - 1
    ):

        signal = detect_engulfing(
            candles,
            idx
        )

        if not signal:
            continue

        features = make_features(
            candles,
            idx,
            signal
        )

        if not features:
            continue

        features["timestamp"] = candles[
            idx
        ]["timestamp"]

        result = evaluate_historical_trade(
            candles,
            idx,
            signal
        )

        if result is None:
            continue

        db.add(
            symbol,
            interval,
            features,
            result,
            candles[idx]["timestamp"]
        )

        added += 1

    db.build_time_stats(
        symbol,
        interval
    )

    log.info(
        "History DB %s %s: candles=%d setups=%d",
        symbol,
        interval,
        len(candles),
        added
    )

    return candles


# ============================================================
# CHART
# ============================================================

def make_chart(
    candles,
    symbol,
    interval,
    entry=None,
    exit_price=None,
    signal=None,
):
    if not candles:
        return None

    data = candles[
        -CONFIG["CHART_CANDLES"]:
    ]

    fig, ax = plt.subplots(
        figsize=(12, 6)
    )

    width = 0.6

    for i, c in enumerate(data):

        o = float(c["open"])
        h = float(c["high"])
        l = float(c["low"])
        cl = float(c["close"])

        ax.plot(
            [i, i],
            [l, h],
            linewidth=1
        )

        if cl >= o:
            bottom = o
            height = cl - o
        else:
            bottom = cl
            height = o - cl

        rect = plt.Rectangle(
            (
                i - width / 2,
                bottom
            ),
            width,
            max(
                height,
                1e-12
            ),
            fill=False
        )

        ax.add_patch(rect)

    if entry is not None:

        ax.axhline(
            entry,
            linestyle="--",
            linewidth=1,
            label="Entry"
        )

    if exit_price is not None:

        ax.axhline(
            exit_price,
            linestyle=":",
            linewidth=1,
            label="Exit"
        )

    ax.set_title(
        f"{symbol} | {interval} | "
        f"{signal or ''}"
    )

    ax.grid(
        True,
        alpha=0.2
    )

    ax.legend()

    buf = io.BytesIO()

    plt.tight_layout()

    fig.savefig(
        buf,
        format="png",
        dpi=150
    )

    plt.close(fig)

    buf.seek(0)

    return buf.getvalue()


# ============================================================
# ENGINE
# ============================================================

class Engine:

    def __init__(
        self,
        symbol,
        interval,
        history_db,
    ):
        self.symbol = normalize_symbol(
            symbol
        )

        self.interval = interval

        self.db = history_db

        self.balance = CONFIG["BALANCE"]
        self.start_balance = self.balance

        self.positions = []

        self.candles = []

        self.trades = []

        self.stats = {
            "wins": 0,
            "losses": 0,
            "signals": 0,
            "blocked": 0,
        }

        self.total_comm = 0.0
        self.gross_pnl = 0.0

        self.consecutive_losses = 0

        self.self_blocked = False

        self.day_start_balance = self.balance

        self.day = datetime.now(
            timezone.utc
        ).date()

        self.last_candle_time = time.time()

        self.pending = {}

        self.last_report = None

    def reset_day(self):

        today = datetime.now(
            timezone.utc
        ).date()

        if today != self.day:

            self.day = today

            self.day_start_balance = (
                self.balance
            )

            self.consecutive_losses = 0
            self.self_blocked = False

    def daily_loss_limit_hit(self):

        loss = (
            self.day_start_balance
            - self.balance
        )

        limit = (
            self.day_start_balance
            * CONFIG[
                "MAX_DAILY_LOSS_PCT"
            ]
        )

        return loss >= limit

    def can_trade(self):

        self.reset_day()

        if self.self_blocked:
            return False

        if self.daily_loss_limit_hit():
            return False

        if (
            len(self.positions)
            >= CONFIG["MAX_OPEN_POS"]
        ):
            return False

        return True

    def calc_lot(
        self,
        entry,
        sl,
        part_risk_ratio=1.0,
    ):

        risk_money = (
            self.balance
            * CONFIG["RISK_PCT"]
            * part_risk_ratio
        )

        sl_dist = abs(
            entry - sl
        )

        if sl_dist <= 0:
            return 0.0

        lot = (
            risk_money
            / sl_dist
        )

        lot = max(
            CONFIG["LOT_MIN"],
            min(
                lot,
                CONFIG["LOT_MAX"]
            )
        )

        return lot

    def open_local(
        self,
        signal,
        entry,
        sl,
    ):

        if signal == "BUY":
            risk = entry - sl
        else:
            risk = sl - entry

        if risk <= 0:
            return None

        lot_a = self.calc_lot(
            entry,
            sl,
            CONFIG["PART_RISK_RATIO"]
        )

        lot_b = self.calc_lot(
            entry,
            sl,
            CONFIG["PART_RISK_RATIO"]
        )

        if lot_a <= 0:
            return None

        p = {
            "id": (
                f"{self.symbol}-"
                f"{self.interval}-"
                f"{int(time.time()*1000)}"
            ),

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

            "tp_a": (
                entry
                + risk * CONFIG["A_TP_R"]
                if signal == "BUY"
                else entry
                - risk * CONFIG["A_TP_R"]
            ),

            "tp_b": None,

            "a_closed": False,

            "b_sl": sl,

            "commission": 0.0,

            "gross": 0.0,

            "opened": datetime.now(
                timezone.utc
            ),

            "trail_r": 0.0,
        }

        open_comm = (
            p["lot"]
            * p["entry"]
            * CONFIG["COMM_RATE"]
        )

        p["commission"] += open_comm

        self.total_comm += open_comm

        self.positions.append(p)

        return p

    def close_signal(
        self,
        p,
        exit_price,
        reason,
        fraction=1.0,
    ):

        if p not in self.positions:
            return

        lot = (
            p["lot"]
            * fraction
        )

        if p["signal"] == "BUY":

            gross = (
                exit_price
                - p["entry"]
            ) * lot

        else:

            gross = (
                p["entry"]
                - exit_price
            ) * lot

        close_comm = (
            lot
            * exit_price
            * CONFIG["COMM_RATE"]
        )

        p["commission"] += close_comm

        self.total_comm += close_comm

        net_pnl = (
            gross
            - close_comm
        )

        self.balance += net_pnl

        self.gross_pnl += gross

        p["gross"] += gross

        self.trades.append({
            "time": datetime.now(
                timezone.utc
            ),
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

        if (
            self.consecutive_losses
            >= CONFIG[
                "MAX_CONSECUTIVE_LOSSES"
            ]
        ):

            self.self_blocked = True

            asyncio.create_task(
                TG_CLIENT.send(
                    f"🛑 <b>SELF BLOCK</b>\n"
                    f"{self.symbol} {self.interval}\n"
                    f"{CONFIG['MAX_CONSECUTIVE_LOSSES']} "
                    f"consecutive losses."
                )
            )

        try:
            self.positions.remove(p)

        except ValueError:
            pass

        return net_pnl

    def manage_local(
        self,
        p,
        price,
    ):

        if p not in self.positions:
            return

        signal = p["signal"]

        entry = p["entry"]
        risk = p["risk"]

        if risk <= 0:
            return

        if signal == "BUY":

            r_now = (
                price - entry
            ) / risk

            if (
                not p["a_closed"]
                and price >= p["tp_a"]
            ):

                old_lot = p["lot"]

                if old_lot > 0:

                    self.close_signal(
                        p,
                        p["tp_a"],
                        "TP_A",
                        fraction=(
                            p["lot_a"]
                            / old_lot
                        )
                    )

                    p["a_closed"] = True

                    if p in self.positions:

                        p["lot"] -= p["lot_a"]

                        p["lot_a"] = 0

                        p["b_sl"] = entry

            if (
                p in self.positions
                and r_now >= CONFIG["BE_AT_R"]
            ):

                p["b_sl"] = max(
                    p["b_sl"],
                    entry
                )

            if (
                p in self.positions
                and r_now >= CONFIG["TRAIL_STEP_R"]
            ):

                trail_r = min(
                    math.floor(
                        r_now
                        / CONFIG["TRAIL_STEP_R"]
                    )
                    * CONFIG["TRAIL_STEP_R"],
                    CONFIG["MAX_TRAIL_R"]
                )

                if trail_r > p["trail_r"]:

                    p["trail_r"] = trail_r

                    new_sl = (
                        entry
                        + risk
                        * max(
                            0,
                            trail_r
                            - CONFIG["TRAIL_STEP_R"]
                        )
                    )

                    p["b_sl"] = max(
                        p["b_sl"],
                        new_sl
                    )

            if (
                p in self.positions
                and price <= p["b_sl"]
            ):

                self.close_signal(
                    p,
                    p["b_sl"],
                    "SL_BE_TRAIL"
                )

        else:

            r_now = (
                entry - price
            ) / risk

            if (
                not p["a_closed"]
                and price <= p["tp_a"]
            ):

                old_lot = p["lot"]

                if old_lot > 0:

                    self.close_signal(
                        p,
                        p["tp_a"],
                        "TP_A",
                        fraction=(
                            p["lot_a"]
                            / old_lot
                        )
                    )

                    p["a_closed"] = True

                    if p in self.positions:

                        p["lot"] -= p["lot_a"]

                        p["lot_a"] = 0

                        p["b_sl"] = entry

            if (
                p in self.positions
                and r_now >= CONFIG["BE_AT_R"]
            ):

                p["b_sl"] = min(
                    p["b_sl"],
                    entry
                )

            if (
                p in self.positions
                and r_now >= CONFIG["TRAIL_STEP_R"]
            ):

                trail_r = min(
                    math.floor(
                        r_now
                        / CONFIG["TRAIL_STEP_R"]
                    )
                    * CONFIG["TRAIL_STEP_R"],
                    CONFIG["MAX_TRAIL_R"]
                )

                if trail_r > p["trail_r"]:

                    p["trail_r"] = trail_r

                    new_sl = (
                        entry
                        - risk
                        * max(
                            0,
                            trail_r
                            - CONFIG["TRAIL_STEP_R"]
                        )
                    )

                    p["b_sl"] = min(
                        p["b_sl"],
                        new_sl
                    )

            if (
                p in self.positions
                and price >= p["b_sl"]
            ):

                self.close_signal(
                    p,
                    p["b_sl"],
                    "SL_BE_TRAIL"
                )

    def manage_positions(
        self,
        price,
    ):

        for p in list(
            self.positions
        ):

            self.manage_local(
                p,
                price
            )

    def history_check(
        self,
        features,
    ):

        return self.db.evaluate(
            self.symbol,
            self.interval,
            features
        )

    async def open_signal(
        self,
        signal,
        candle,
    ):

        if not self.can_trade():

            self.stats["blocked"] += 1

            return

        self.stats["signals"] += 1

        entry = float(
            candle["close"]
        )

        if signal == "BUY":

            sl = float(
                candle["low"]
            )

        else:

            sl = float(
                candle["high"]
            )

        buf = (
            abs(entry)
            * CONFIG["SL_BUF"]
            / 100000
        )

        if signal == "BUY":
            sl -= buf
        else:
            sl += buf

        features = make_features(
            self.candles,
            len(self.candles) - 1,
            signal
        )

        if not features:
            return

        features["timestamp"] = candle[
            "timestamp"
        ]

        result = self.history_check(
            features
        )

        if not result["ok"]:

            log.info(
                "FILTER %s %s %s: %s",
                self.symbol,
                self.interval,
                signal,
                result["reason"]
            )

            return

        p = self.open_local(
            signal,
            entry,
            sl
        )

        if not p:
            return

        msg = (
            f"🟢 <b>{signal}</b>\n"
            f"<b>{self.symbol}</b> | "
            f"{self.interval}\n"
            f"Entry: {entry:.6f}\n"
            f"SL: {sl:.6f}\n"
            f"TP A: {p['tp_a']:.6f}\n"
            f"Similar: {result['count']}\n"
            f"Good: {result['good_pct']:.2%}\n"
            f"Balance: {self.balance:.2f}"
        )

        await TG_CLIENT.send(
            msg
        )

        chart = make_chart(
            self.candles,
            self.symbol,
            self.interval,
            entry=entry,
            signal=signal,
        )

        if chart:

            await TG_CLIENT.send_chart(
                chart,
                caption=(
                    f"{self.symbol} "
                    f"{self.interval} "
                    f"{signal}"
                )
            )

    async def handle_closed(
        self,
        candle,
    ):

        self.last_candle_time = time.time()

        if not self.candles:

            self.candles.append(
                candle
            )

        else:

            last = self.candles[-1]

            if (
                last["timestamp"]
                == candle["timestamp"]
            ):

                self.candles[-1] = candle

            else:

                self.candles.append(
                    candle
                )

        self.manage_positions(
            float(candle["close"])
        )

        if len(self.candles) < 2:
            return

        idx = len(
            self.candles
        ) - 1

        signal = detect_engulfing(
            self.candles,
            idx
        )

        if signal:

            await self.open_signal(
                signal,
                candle
            )

        max_keep = max(
            CONFIG["HISTORY_LIMIT"],
            CONFIG["PRELOAD_CANDLES"]
        )

        if len(self.candles) > max_keep:

            self.candles = self.candles[
                -max_keep:
            ]

    async def handle_realtime(
        self,
        candle,
    ):

        self.last_candle_time = time.time()

        price = float(
            candle["close"]
        )

        self.manage_positions(
            price
        )

        if not CONFIG[
            "REALTIME_ENTRY"
        ]:
            return

        if len(self.candles) < 2:
            return

        test_candles = list(
            self.candles
        )

        if (
            test_candles
            and test_candles[-1][
                "timestamp"
            ] == candle["timestamp"]
        ):

            test_candles[-1] = candle

        else:

            test_candles.append(
                candle
            )

        if len(test_candles) < 2:
            return

        idx = len(
            test_candles
        ) - 1

        signal = detect_engulfing(
            test_candles,
            idx
        )

        if not signal:
            return

        key = signal

        now = time.time()

        pending = self.pending.get(
            key
        )

        if pending is None:

            self.pending[key] = {
                "started": now,
                "ticks": 1,
                "candle_time": candle[
                    "timestamp"
                ],
            }

            return

        if (
            pending["candle_time"]
            != candle["timestamp"]
        ):

            self.pending[key] = {
                "started": now,
                "ticks": 1,
                "candle_time": candle[
                    "timestamp"
                ],
            }

            return

        pending["ticks"] += 1

        elapsed = (
            now
            - pending["started"]
        )

        if (
            pending["ticks"]
            >= CONFIG["CONFIRM_TICKS"]
            and elapsed
            >= CONFIG["CONFIRM_SECONDS"]
        ):

            candle_key = (
                f"{candle['timestamp']}-"
                f"{signal}"
            )

            if (
                self.pending.get(
                    "executed"
                ) == candle_key
            ):
                return

            self.pending[
                "executed"
            ] = candle_key

            old = self.candles

            self.candles = test_candles

            try:

                await self.open_signal(
                    signal,
                    candle
                )

            finally:

                self.candles = old


# ============================================================
# GLOBAL ENGINES
# ============================================================

ENGINES = {}


# ============================================================
# PRELOAD CURRENT
# ============================================================

async def preload_current(
    client,
    engine,
):

    try:

        data = await client.time_series(
            symbol=engine.symbol,
            interval=engine.interval,
            outputsize=(
                CONFIG["PRELOAD_CANDLES"]
                + 2
            ),
        )

        values = data.get(
            "values",
            []
        )

        candles = td_values_to_candles(
            values
        )

        if candles:

            engine.candles = candles[
                -CONFIG[
                    "PRELOAD_CANDLES"
                ]:
            ]

            engine.last_candle_time = (
                time.time()
            )

            log.info(
                "Preload %s %s: %d candles",
                engine.symbol,
                engine.interval,
                len(engine.candles)
            )

    except Exception as e:

        log.error(
            "Preload error %s %s: %s",
            engine.symbol,
            engine.interval,
            e
        )


# ============================================================
# LIVE CANDLE BUILDER
# ============================================================

LIVE_CANDLES = {}


def candle_bucket(
    timestamp,
    interval,
):

    sec = interval_seconds(
        interval
    )

    ts = int(
        timestamp.timestamp()
    )

    bucket = (
        ts // sec
    ) * sec

    return datetime.fromtimestamp(
        bucket,
        tz=timezone.utc
    )


async def process_price_tick(
    symbol,
    price,
    timestamp=None,
):

    symbol = normalize_symbol(
        symbol
    )

    if timestamp is None:

        timestamp = datetime.now(
            timezone.utc
        )

    if timestamp.tzinfo is None:

        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    price = float(price)

    for (
        key,
        engine
    ) in list(
        ENGINES.items()
    ):

        if engine.symbol != symbol:
            continue

        interval = engine.interval

        bucket = candle_bucket(
            timestamp,
            interval
        )

        live_key = (
            symbol,
            interval
        )

        current = LIVE_CANDLES.get(
            live_key
        )

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

            LIVE_CANDLES[
                live_key
            ] = current

            await engine.handle_realtime(
                current
            )

            continue

        if (
            current["timestamp"]
            == bucket
        ):

            current["high"] = max(
                current["high"],
                price
            )

            current["low"] = min(
                current["low"],
                price
            )

            current["close"] = price

            await engine.handle_realtime(
                current
            )

            continue

        previous = dict(
            current
        )

        previous["closed"] = True

        await engine.handle_closed(
            previous
        )

        new_candle = {
            "timestamp": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
            "closed": False,
        }

        LIVE_CANDLES[
            live_key
        ] = new_candle

        await engine.handle_realtime(
            new_candle
        )


# ============================================================
# PRICE WEBSOCKET
# ============================================================

async def market_stream(
    client,
):

    symbols = [
        normalize_symbol(x)
        for x in CONFIG[
            "SYMBOLS"
        ]
    ]

    reconnect_delay = 3

    while True:

        try:

            log.info(
                "Connecting Twelve Data price stream..."
            )

            async for raw in client.price_stream(
                symbols
            ):

                try:

                    if isinstance(
                        raw,
                        bytes
                    ):

                        raw = raw.decode(
                            "utf-8",
                            errors="ignore"
                        )

                    msg = json.loads(
                        raw
                    )

                    if not isinstance(
                        msg,
                        dict
                    ):
                        continue

                    if msg.get(
                        "event"
                    ) in (
                        "subscribe-status",
                        "heartbeat",
                    ):
                        continue

                    symbol = (
                        msg.get("symbol")
                        or msg.get("code")
                    )

                    price = (
                        msg.get("price")
                        or msg.get("close")
                        or msg.get("value")
                    )

                    if (
                        not symbol
                        or price is None
                    ):
                        continue

                    ts_value = (
                        msg.get("timestamp")
                        or msg.get("time")
                    )

                    ts = None

                    if ts_value is not None:

                        try:

                            if isinstance(
                                ts_value,
                                (int, float)
                            ):

                                ts = datetime.fromtimestamp(
                                    float(ts_value),
                                    tz=timezone.utc
                                )

                            else:

                                ts = pd.to_datetime(
                                    ts_value,
                                    utc=True
                                ).to_pydatetime()

                        except Exception:

                            ts = None

                    await process_price_tick(
                        symbol,
                        float(price),
                        ts
                    )

                except Exception as e:

                    log.error(
                        "Price message error: %s",
                        e
                    )

        except asyncio.CancelledError:

            raise

        except Exception as e:

            log.error(
                "Market stream disconnected: %s",
                e
            )

            await asyncio.sleep(
                reconnect_delay
            )

            reconnect_delay = min(
                reconnect_delay * 2,
                60
            )


# ============================================================
# REPORT
# ============================================================

async def daily_report():

    lines = [
        "📊 <b>DAILY REPORT</b>",
        "",
    ]

    total_balance = 0.0
    total_pnl = 0.0
    total_trades = 0

    for engine in ENGINES.values():

        pnl = (
            engine.balance
            - CONFIG["BALANCE"]
        )

        total_balance += engine.balance
        total_pnl += pnl

        total_trades += len(
            engine.trades
        )

        lines.append(
            f"<b>{engine.symbol} "
            f"{engine.interval}</b> | "
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

    await TG_CLIENT.send(
        "\n".join(lines)
    )


# ============================================================
# HEALTH CHECK
# ============================================================

async def health_check():

    while True:

        try:

            now = time.time()

            dead = []

            for key, engine in ENGINES.items():

                age = (
                    now
                    - engine.last_candle_time
                )

                if age > (
                    CONFIG[
                        "SOCKET_TIMEOUT_MIN"
                    ] * 60
                ):

                    dead.append(
                        (
                            engine.symbol,
                            engine.interval,
                            age
                        )
                    )

            if dead:

                text = (
                    "⚠️ <b>MARKET DATA WARNING</b>\n"
                )

                for (
                    symbol,
                    interval,
                    age
                ) in dead:

                    text += (
                        f"{symbol} {interval}: "
                        f"{age/60:.1f} min\n"
                    )

                await TG_CLIENT.send(
                    text
                )

        except Exception as e:

            log.error(
                "Health error: %s",
                e
            )

        await asyncio.sleep(60)


# ============================================================
# DIAGNOSTICS
# ============================================================

async def diagnostics_loop():

    last_day = None

    while True:

        try:

            now = datetime.now(
                timezone.utc
            )

            if (
                now.hour
                == CONFIG[
                    "DIAGNOSTICS_HOUR"
                ]
                and now.minute < 5
                and last_day != now.date()
            ):

                last_day = now.date()

                lines = [
                    "🔎 <b>BOT DIAGNOSTICS</b>",
                    "",
                ]

                for engine in ENGINES.values():

                    lines.append(
                        f"{engine.symbol} "
                        f"{engine.interval}: "
                        f"candles={len(engine.candles)}, "
                        f"positions={len(engine.positions)}, "
                        f"signals={engine.stats['signals']}, "
                        f"blocked={engine.self_blocked}"
                    )

                await TG_CLIENT.send(
                    "\n".join(lines)
                )

        except Exception as e:

            log.error(
                "Diagnostics error: %s",
                e
            )

        await asyncio.sleep(30)


# ============================================================
# REPORT LOOP
# ============================================================

async def report_loop():

    last_day = None

    while True:

        try:

            now = datetime.now(
                timezone.utc
            )

            if (
                now.hour
                == CONFIG["REPORT_HOUR"]
                and now.minute < 5
                and last_day != now.date()
            ):

                last_day = now.date()

                await daily_report()

        except Exception as e:

            log.error(
                "Report loop error: %s",
                e
            )

        await asyncio.sleep(30)


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TWELVE_DATA_API_KEY:

        raise RuntimeError(
            "TWELVE_DATA_API_KEY is missing"
        )

    if not TELEGRAM_TOKEN:

        log.warning(
            "TELEGRAM_TOKEN is missing"
        )

    if not TELEGRAM_CHAT_ID:

        log.warning(
            "TELEGRAM_CHAT_ID is missing"
        )

    log.info(
        "=================================================="
    )

    log.info(
        "EngulfingTrend Bot v6.0.0"
    )

    log.info(
        "Market data: Twelve Data"
    )

    log.info(
        "Symbols: %s",
        CONFIG["SYMBOLS"]
    )

    log.info(
        "Intervals: %s",
        CONFIG["INTERVALS"]
    )

    log.info(
        "Commission: %.8f",
        CONFIG["COMM_RATE"]
    )

    log.info(
        "=================================================="
    )

    client = TwelveData(
        TWELVE_DATA_API_KEY
    )

    db = HistoricalDB()

    try:

        await client.start()

        # ----------------------------------------------------
        # BUILD 6 MONTH HISTORY
        # ----------------------------------------------------

        for symbol in CONFIG[
            "SYMBOLS"
        ]:

            for interval in CONFIG[
                "INTERVALS"
            ]:

                symbol = normalize_symbol(
                    symbol
                )

                log.info(
                    "Building history: %s %s",
                    symbol,
                    interval
                )

                await build_history_db(
                    client,
                    db,
                    symbol,
                    interval
                )

        # ----------------------------------------------------
        # CREATE ENGINES
        # ----------------------------------------------------

        for symbol in CONFIG[
            "SYMBOLS"
        ]:

            symbol = normalize_symbol(
                symbol
            )

            for interval in CONFIG[
                "INTERVALS"
            ]:

                engine = Engine(
                    symbol,
                    interval,
                    db
                )

                ENGINES[
                    (
                        symbol,
                        interval
                    )
                ] = engine

                await preload_current(
                    client,
                    engine
                )

        # ----------------------------------------------------
        # TELEGRAM START
        # ----------------------------------------------------

        await TG_CLIENT.send(
            "🟢 <b>EngulfingTrend Bot v6.0.0 STARTED</b>\n\n"
            f"Market: Twelve Data\n"
            f"Symbols: {', '.join(CONFIG['SYMBOLS'])}\n"
            f"Timeframes: {', '.join(CONFIG['INTERVALS'])}\n"
            f"Commission: {CONFIG['COMM_RATE']}\n"
            f"Historical DB: {CONFIG['HISTORY_MONTHS']} months"
        )

        # ----------------------------------------------------
        # RUN ALL TASKS
        # ----------------------------------------------------

        tasks = [
            asyncio.create_task(
                market_stream(client)
            ),

            asyncio.create_task(
                health_check()
            ),

            asyncio.create_task(
                diagnostics_loop()
            ),

            asyncio.create_task(
                report_loop()
            ),
        ]

        await asyncio.gather(
            *tasks
        )

    finally:

        await client.close()

        log.info(
            "Bot stopped."
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        log.info(
            "Stopped by user."
        )

    except Exception as e:

        log.exception(
            "FATAL ERROR: %s",
            e
        )