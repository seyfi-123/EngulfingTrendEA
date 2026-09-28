# ============================================================
# EngulfingTrend Bot v6.0.0
#
# REALTIME PRICE ACTION
# + OPEN/CLOSE/HIGH/LOW
# + LAST 10 CANDLE STRUCTURE
# + 6 MONTH HISTORICAL SIMILARITY
# + TIME-WINDOW FILTER
# + 50% GOOD HISTORICAL SETUPS
# + 10 LOSS SELF-BLOCK
# + TOTAL RISK 2% PER SIGNAL
# + A = 1% / TP 2R
# + B = 1% / BE 2R / TRAILING
#
# ADDITIONS:
# 1) 4 INDEPENDENT TIMEFRAMES
# 2) TELEGRAM CLOSE CHART
# 3) LIVE CANDLE HISTORY FILTER
#
# IMPORTANT:
# Historical filter uses ONLY data before the current setup.
# No future candles are used for deciding a live entry.
# ============================================================

import os
import io
import math
import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager

from telegram import Bot, InputFile
from telegram.constants import ParseMode


load_dotenv()


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    # ---------------- BASIC ----------------
    "SYMBOLS": [
        s.strip().upper()
        for s in os.getenv(
            "SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT"
        ).split(",")
        if s.strip()
    ],

    # ========================================================
    # ADDITION 1:
    # 4 independent timeframes
    #
    # Example:
    # INTERVALS=1m,5m,15m,1h
    #
    # If INTERVALS is not provided, old INTERVAL is used.
    # ========================================================

    "INTERVAL": os.getenv("INTERVAL", "1m"),

    "INTERVALS": [
        s.strip()
        for s in os.getenv(
            "INTERVALS",
            os.getenv("INTERVAL", "1m,5m,15m,1h")
        ).split(",")
        if s.strip()
    ],

    "BALANCE": float(os.getenv("BALANCE", "1000")),
    "RISK_PCT": float(os.getenv("RISK_PCT", "0.02")),

    # Each signal = 2% TOTAL risk
    # A = 1%
    # B = 1%
    "PART_RISK_RATIO": float(os.getenv("PART_RISK_RATIO", "0.50")),

    "LOT_MIN": float(os.getenv("LOT_MIN", "0.001")),
    "LOT_MAX": float(os.getenv("LOT_MAX", "5.0")),

    "MIN_RISK_USD": float(os.getenv("MIN_RISK_USD", "1.0")),

    "MAX_OPEN_POS": int(os.getenv("MAX_OPEN_POS", "10")),

    "PRELOAD_CANDLES": int(
        os.getenv("PRELOAD_CANDLES", "300")
    ),

    "SL_BUF": float(os.getenv("SL_BUF", "10")),

    # ---------------- ENTRY ----------------
    "REALTIME_ENTRY": os.getenv(
        "REALTIME_ENTRY", "True"
    ).lower() == "true",

    "CONFIRM_SECONDS": float(
        os.getenv("CONFIRM_SECONDS", "2.5")
    ),

    "CONFIRM_TICKS": int(
        os.getenv("CONFIRM_TICKS", "2")
    ),

    # ---------------- RR ----------------
    "A_TP_R": float(os.getenv("A_TP_R", "2.0")),

    "BE_AT_R": float(os.getenv("BE_AT_R", "2.0")),

    "TRAIL_STEP_R": float(
        os.getenv("TRAIL_STEP_R", "2.0")
    ),

    "MAX_TRAIL_R": float(
        os.getenv("MAX_TRAIL_R", "10.0")
    ),

    # ---------------- COST ----------------
    "COMM_RATE": float(
        os.getenv("COMM_RATE", "0.0005")
    ),

    # ---------------- HISTORY ----------------
    "HISTORY_MONTHS": int(
        os.getenv("HISTORY_MONTHS", "6")
    ),

    "HISTORY_LOOKBACK_CANDLES": int(
        os.getenv("HISTORY_LOOKBACK_CANDLES", "10")
    ),

    "MIN_SIMILAR": int(
        os.getenv("MIN_SIMILAR", "25")
    ),

    "SIMILAR_GOOD_PCT": float(
        os.getenv("SIMILAR_GOOD_PCT", "0.40")
    ),

    # Similarity tolerance
    "SIM_TOL_BODY": float(
        os.getenv("SIM_TOL_BODY", "0.55")
    ),

    "SIM_TOL_RANGE": float(
        os.getenv("SIM_TOL_RANGE", "0.25")
    ),

    "SIM_TOL_WICK": float(
        os.getenv("SIM_TOL_WICK", "0.55")
    ),

    "SIM_TOL_STRUCTURE": float(
        os.getenv("SIM_TOL_STRUCTURE", "0.25")
    ),

    "SIM_TOL_DISTANCE": float(
        os.getenv("SIM_TOL_DISTANCE", "0.20")
    ),

    # ---------------- TIME FILTER ----------------
    "TIME_BUCKET_MINUTES": int(
        os.getenv("TIME_BUCKET_MINUTES", "60")
    ),

    "TIME_MIN_SAMPLES": int(
        os.getenv("TIME_MIN_SAMPLES", "20")
    ),

    "TIME_MIN_GOOD_PCT": float(
        os.getenv("TIME_MIN_GOOD_PCT", "0.20")
    ),

    # ---------------- SELF BLOCK ----------------
    "MAX_CONSECUTIVE_LOSSES": int(
        os.getenv("MAX_CONSECUTIVE_LOSSES", "10")
    ),

    # ---------------- REPORT ----------------
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

    # Historical download batch
    "HISTORY_LIMIT": 1000,

    # How many historical candidates to inspect after
    # rough filtering
    "MAX_HISTORY_CANDIDATES": int(
        os.getenv("MAX_HISTORY_CANDIDATES", "5000")
    ),
}


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

log = logging.getLogger(__name__)


# ============================================================
# TELEGRAM
# ============================================================

class TG:

    def __init__(self, token, chat_id):
        self.bot = Bot(token=token) if token else None
        self.chat_id = chat_id

    async def send(self, msg):

        if not self.bot:
            return

        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=msg,
                parse_mode=ParseMode.HTML
            )

        except Exception as e:
            log.error(f"TG send: {e}")

    async def photo(self, buf, caption=""):

        if not self.bot:
            return

        try:
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=InputFile(
                    buf,
                    filename="chart.png"
                ),
                caption=caption,
                parse_mode=ParseMode.HTML
            )

        except Exception as e:
            log.error(f"TG photo: {e}")


tg = TG(
    TELEGRAM_TOKEN,
    TELEGRAM_CHAT_ID
)


# ============================================================
# BASIC CANDLE HELPERS
# ============================================================

def candle_range(c):
    return max(
        float(c["high"]) - float(c["low"]),
        1e-12
    )


def body_size(c):
    return abs(
        float(c["close"]) -
        float(c["open"])
    )


def body_ratio(c):
    r = candle_range(c)
    return body_size(c) / r


def upper_wick_ratio(c):

    r = candle_range(c)

    upper = float(c["high"]) - max(
        float(c["open"]),
        float(c["close"])
    )

    return max(0.0, upper) / r


def lower_wick_ratio(c):

    r = candle_range(c)

    lower = min(
        float(c["open"]),
        float(c["close"])
    ) - float(c["low"])

    return max(0.0, lower) / r


def close_position(c):

    r = candle_range(c)

    return (
        float(c["close"]) -
        float(c["low"])
    ) / r


def signed_body(c):

    r = candle_range(c)

    return (
        float(c["close"]) -
        float(c["open"])
    ) / r


# ============================================================
# STRUCTURE
# ============================================================

def structure_features(candles):

    if len(candles) < 10:
        return {
            "trend": 0.0,
            "hh": 0.0,
            "hl": 0.0,
            "lh": 0.0,
            "ll": 0.0,
            "range_pos": 0.5,
        }

    last10 = candles[-10:]

    highs = [
        float(x["high"])
        for x in last10
    ]

    lows = [
        float(x["low"])
        for x in last10
    ]

    closes = [
        float(x["close"])
        for x in last10
    ]

    hh = 0
    hl = 0
    lh = 0
    ll = 0

    for i in range(2, len(last10)):

        if highs[i] > highs[i - 1]:
            hh += 1

        if lows[i] > lows[i - 1]:
            hl += 1

        if highs[i] < highs[i - 1]:
            lh += 1

        if lows[i] < lows[i - 1]:
            ll += 1

    up_score = hh + hl
    down_score = lh + ll

    trend = (
        up_score - down_score
    ) / max(
        1,
        up_score + down_score
    )

    highest = max(highs)
    lowest = min(lows)

    range_pos = (
        closes[-1] - lowest
    ) / max(
        highest - lowest,
        1e-12
    )

    return {
        "trend": trend,
        "hh": hh / 8.0,
        "hl": hl / 8.0,
        "lh": lh / 8.0,
        "ll": ll / 8.0,
        "range_pos": range_pos,
    }


# ============================================================
# ENGULFING
# ============================================================

def detect_engulfing(candles, idx):

    if idx < 2:
        return None

    cur = candles[idx]
    p1 = candles[idx - 1]
    p2 = candles[idx - 2]

    top = max(
        float(p1["close"]),
        float(p2["close"])
    )

    bottom = min(
        float(p1["close"]),
        float(p2["close"])
    )

    # ---------------- BULL ----------------

    bull = (
        float(cur["close"]) >
        float(cur["open"])
        and
        float(p1["close"]) <
        float(p1["open"])
        and
        float(p2["close"]) <
        float(p2["open"])
        and
        float(cur["open"]) < bottom
        and
        float(cur["close"]) > top
    )

    if bull:

        return {
            "type": "B",
            "candles": 2,
            "trigger": top,
        }

    # ---------------- BEAR ----------------

    bear = (
        float(cur["close"]) <
        float(cur["open"])
        and
        float(p1["close"]) >
        float(p1["open"])
        and
        float(p2["close"]) >
        float(p2["open"])
        and
        float(cur["open"]) > top
        and
        float(cur["close"]) < bottom
    )

    if bear:

        return {
            "type": "S",
            "candles": 2,
            "trigger": bottom,
        }

    return None


# ============================================================
# FEATURE EXTRACTION
# ============================================================

def make_features(candles, idx, signal):

    if idx < 10:
        return None

    cur = candles[idx]
    p1 = candles[idx - 1]
    p2 = candles[idx - 2]

    recent = candles[
        idx - CONFIG["HISTORY_LOOKBACK_CANDLES"] + 1:
        idx + 1
    ]

    if len(recent) < CONFIG["HISTORY_LOOKBACK_CANDLES"]:
        return None

    sf = structure_features(recent)

    entry = float(cur["close"])

    if signal["type"] == "B":

        trigger_distance = (
            entry - signal["trigger"]
        ) / max(
            entry,
            1e-12
        )

        sl_base = min(
            float(cur["low"]),
            float(p1["low"]),
            float(p2["low"])
        )

    else:

        trigger_distance = (
            signal["trigger"] - entry
        ) / max(
            entry,
            1e-12
        )

        sl_base = max(
            float(cur["high"]),
            float(p1["high"]),
            float(p2["high"])
        )

    price_range = (
        max(
            float(x["high"])
            for x in recent
        )
        -
        min(
            float(x["low"])
            for x in recent
        )
    )

    range_pct = (
        price_range /
        max(entry, 1e-12)
    )

    return {
        "type": signal["type"],

        "body": body_ratio(cur),

        "upper_wick": upper_wick_ratio(cur),

        "lower_wick": lower_wick_ratio(cur),

        "close_pos": close_position(cur),

        "signed_body": signed_body(cur),

        "p1_body": body_ratio(p1),

        "p2_body": body_ratio(p2),

        "range_pct": range_pct,

        "trigger_distance": trigger_distance,

        "trend": sf["trend"],

        "hh": sf["hh"],

        "hl": sf["hl"],

        "lh": sf["lh"],

        "ll": sf["ll"],

        "range_pos": sf["range_pos"],

        "sl_base": sl_base,

        "entry": entry,

        "time": int(cur["time"]),
    }


# ============================================================
# SIMILARITY
# ============================================================

def normalized_distance(a, b, tolerance):

    if tolerance <= 0:
        tolerance = 0.01

    return abs(a - b) / tolerance


def feature_distance(a, b):

    if a["type"] != b["type"]:
        return 999.0

    values = []

    values.append(
        normalized_distance(
            a["body"],
            b["body"],
            CONFIG["SIM_TOL_BODY"]
        )
    )

    values.append(
        normalized_distance(
            a["upper_wick"],
            b["upper_wick"],
            CONFIG["SIM_TOL_WICK"]
        )
    )

    values.append(
        normalized_distance(
            a["lower_wick"],
            b["lower_wick"],
            CONFIG["SIM_TOL_WICK"]
        )
    )

    values.append(
        normalized_distance(
            a["close_pos"],
            b["close_pos"],
            CONFIG["SIM_TOL_WICK"]
        )
    )

    values.append(
        normalized_distance(
            a["p1_body"],
            b["p1_body"],
            CONFIG["SIM_TOL_BODY"]
        )
    )

    values.append(
        normalized_distance(
            a["p2_body"],
            b["p2_body"],
            CONFIG["SIM_TOL_BODY"]
        )
    )

    values.append(
        normalized_distance(
            a["range_pct"],
            b["range_pct"],
            CONFIG["SIM_TOL_RANGE"]
        )
    )

    values.append(
        normalized_distance(
            a["trigger_distance"],
            b["trigger_distance"],
            CONFIG["SIM_TOL_DISTANCE"]
        )
    )

    values.append(
        normalized_distance(
            a["trend"],
            b["trend"],
            CONFIG["SIM_TOL_STRUCTURE"]
        )
    )

    values.append(
        normalized_distance(
            a["hh"],
            b["hh"],
            CONFIG["SIM_TOL_STRUCTURE"]
        )
    )

    values.append(
        normalized_distance(
            a["hl"],
            b["hl"],
            CONFIG["SIM_TOL_STRUCTURE"]
        )
    )

    values.append(
        normalized_distance(
            a["lh"],
            b["lh"],
            CONFIG["SIM_TOL_STRUCTURE"]
        )
    )

    values.append(
        normalized_distance(
            a["ll"],
            b["ll"],
            CONFIG["SIM_TOL_STRUCTURE"]
        )
    )

    return sum(values) / len(values)


# ============================================================
# HISTORICAL OUTCOME
# ============================================================

def evaluate_historical_trade(
    candles,
    signal_idx,
    signal,
):

    if signal_idx >= len(candles):
        return None

    cur = candles[signal_idx]

    entry = float(cur["close"])

    # Historical SL = same structural principle
    if signal["type"] == "B":

        sl = min(
            float(cur["low"]),
            float(candles[signal_idx - 1]["low"]),
            float(candles[signal_idx - 2]["low"])
        )

        risk = entry - sl

        if risk <= 0:
            return None

        tp_a = entry + (
            CONFIG["A_TP_R"] * risk
        )

    else:

        sl = max(
            float(cur["high"]),
            float(candles[signal_idx - 1]["high"]),
            float(candles[signal_idx - 2]["high"])
        )

        risk = sl - entry

        if risk <= 0:
            return None

        tp_a = entry - (
            CONFIG["A_TP_R"] * risk
        )

    # Need enough future candles
    end = min(
        len(candles),
        signal_idx + 1000
    )

    be_reached = False
    max_reached_r = 0.0

    # We use candle-level simulation.
    #
    # If both SL and TP are inside the same candle,
    # conservative ordering = SL first.
    #
    # This avoids artificially inflating historical results.
    for j in range(
        signal_idx + 1,
        end
    ):

        c = candles[j]

        high = float(c["high"])
        low = float(c["low"])

        if signal["type"] == "B":

            max_r = (
                high - entry
            ) / risk

            max_reached_r = max(
                max_reached_r,
                max_r
            )

            # Stop first
            if low <= sl:
                return -1.0

            # A TP
            if high >= tp_a:
                return CONFIG["A_TP_R"]

            if max_r >= CONFIG["MAX_TRAIL_R"]:
                return CONFIG["MAX_TRAIL_R"]

            if max_r >= CONFIG["BE_AT_R"]:
                be_reached = True

            if be_reached:

                steps = int(
                    max_r /
                    CONFIG["TRAIL_STEP_R"]
                )

                lock_r = (
                    steps - 1
                ) * CONFIG["TRAIL_STEP_R"]

                if lock_r > 0:

                    trail_sl = (
                        entry +
                        lock_r * risk
                    )

                    if low <= trail_sl:
                        return lock_r

        else:

            max_r = (
                entry - low
            ) / risk

            max_reached_r = max(
                max_reached_r,
                max_r
            )

            if high >= sl:
                return -1.0

            if low <= tp_a:
                return CONFIG["A_TP_R"]

            if max_r >= CONFIG["MAX_TRAIL_R"]:
                return CONFIG["MAX_TRAIL_R"]

            if max_r >= CONFIG["BE_AT_R"]:
                be_reached = True

            if be_reached:

                steps = int(
                    max_r /
                    CONFIG["TRAIL_STEP_R"]
                )

                lock_r = (
                    steps - 1
                ) * CONFIG["TRAIL_STEP_R"]

                if lock_r > 0:

                    trail_sl = (
                        entry -
                        lock_r * risk
                    )

                    if high >= trail_sl:
                        return lock_r

    return max_reached_r if max_reached_r > 0 else -1.0


# ============================================================
# HISTORICAL DATABASE
# ============================================================

class HistoricalDB:

    def __init__(self, symbol, interval):

        self.symbol = symbol

        # ====================================================
        # ADDITION 1:
        # DB is independent for every symbol + timeframe.
        # ====================================================
        self.interval = interval

        self.rows = []

        self.time_stats = {}

        self.ready = False

        self.loaded_from = None
        self.loaded_to = None

    # --------------------------------------------------------

    def time_bucket(self, timestamp):

        dt = datetime.fromtimestamp(
            timestamp,
            timezone.utc
        )

        minute = (
            dt.hour * 60 +
            dt.minute
        )

        bucket = (
            minute //
            CONFIG["TIME_BUCKET_MINUTES"]
        )

        return bucket

    # --------------------------------------------------------

    def build_time_stats(self):

        groups = {}

        for row in self.rows:

            bucket = row["time_bucket"]

            if bucket not in groups:
                groups[bucket] = []

            groups[bucket].append(
                row["r"]
            )

        self.time_stats = {}

        for bucket, results in groups.items():

            if not results:
                continue

            good = sum(
                1
                for r in results
                if r > 0
            )

            avg_r = (
                sum(results) /
                len(results)
            )

            self.time_stats[bucket] = {
                "count": len(results),
                "good": good,
                "good_pct": good / len(results),
                "avg_r": avg_r,
            }

    # --------------------------------------------------------

    def add(
        self,
        features,
        result_r
    ):

        row = dict(features)

        row["r"] = result_r

        row["good"] = (
            result_r > 0
        )

        row["time_bucket"] = (
            self.time_bucket(
                features["time"]
            )
        )

        self.rows.append(row)

    # --------------------------------------------------------

    def similar(
        self,
        current_features
    ):

        candidates = []

        # Newest historical records first
        # because recent market behavior
        # should have priority when distance
        # is similar.
        source = self.rows[
            -CONFIG["MAX_HISTORY_CANDIDATES"]:
        ]

        for row in source:

            d = feature_distance(
                current_features,
                row
            )

            candidates.append(
                (d, row)
            )

        candidates.sort(
            key=lambda x: x[0]
        )

        # Dynamic tolerance:
        # take sufficiently similar setups.
        #
        # We don't force an arbitrary fixed
        # number if they are not actually similar.
        selected = []

        for distance, row in candidates:

            if distance <= 1.0:

                selected.append(
                    (distance, row)
                )

            if len(selected) >= 200:
                break

        return selected

    # --------------------------------------------------------

    def evaluate(
        self,
        current_features
    ):

        matches = self.similar(
            current_features
        )

        count = len(matches)

        if count < CONFIG["MIN_SIMILAR"]:

            return {
                "allow": False,
                "reason": "SIMILAR_TOO_FEW",
                "count": count,
                "good_pct": 0.0,
                "avg_r": 0.0,
                "time_ok": False,
            }

        results = [
            row["r"]
            for _, row in matches
        ]

        good = sum(
            1
            for r in results
            if r > 0
        )

        good_pct = (
            good / count
        )

        avg_r = (
            sum(results) /
            count
        )

        bucket = self.time_bucket(
            current_features["time"]
        )

        time_stat = self.time_stats.get(
            bucket
        )

        time_ok = True

        if time_stat:

            if (
                time_stat["count"] >=
                CONFIG["TIME_MIN_SAMPLES"]
            ):

                time_ok = (
                    time_stat["good_pct"] >=
                    CONFIG["TIME_MIN_GOOD_PCT"]
                    and
                    time_stat["avg_r"] > 0
                )

        allow = (
            good_pct >=
            CONFIG["SIMILAR_GOOD_PCT"]
            and
            avg_r > 0
            and
            time_ok
        )

        return {
            "allow": allow,
            "reason": (
                "PASS"
                if allow
                else "HISTORY_FILTER"
            ),
            "count": count,
            "good_pct": good_pct,
            "avg_r": avg_r,
            "time_ok": time_ok,
            "time_stat": time_stat,
            "matches": matches[:20],
        }


# ============================================================
# HISTORICAL DOWNLOAD
# ============================================================

async def download_6m_history(
    client,
    symbol,
    interval
):

    now_ms = int(
        time.time() * 1000
    )

    start_dt = (
        datetime.now(timezone.utc)
        -
        timedelta(
            days=30 *
            CONFIG["HISTORY_MONTHS"]
        )
    )

    start_ms = int(
        start_dt.timestamp() * 1000
    )

    end_ms = now_ms

    all_klines = []

    cursor = start_ms

    log.info(
        f"📚 {symbol} [{interval}]: "
        f"{CONFIG['HISTORY_MONTHS']} oy history "
        f"yuklanmoqda..."
    )

    while cursor < end_ms:

        try:

            klines = await client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=cursor,
                endTime=end_ms,
                limit=CONFIG["HISTORY_LIMIT"]
            )

        except Exception as e:

            log.error(
                f"{symbol} [{interval}]: "
                f"history request xato: {e}"
            )

            await asyncio.sleep(2)

            continue

        if not klines:
            break

        all_klines.extend(
            klines
        )

        last_open_time = int(
            klines[-1][0]
        )

        next_cursor = (
            last_open_time +
            1
        )

        if next_cursor <= cursor:
            break

        cursor = next_cursor

        if len(all_klines) % 10000 == 0:

            log.info(
                f"📚 {symbol} [{interval}]: "
                f"{len(all_klines)} candle..."
            )

        # Binance rate safety
        await asyncio.sleep(
            0.08
        )

    candles = []

    for k in all_klines:

        candles.append({
            "time": int(k[0] // 1000),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "closed": True,
        })

    # Deduplicate
    unique = {}

    for c in candles:
        unique[c["time"]] = c

    candles = sorted(
        unique.values(),
        key=lambda x: x["time"]
    )

    log.info(
        f"📚 {symbol} [{interval}]: "
        f"{len(candles)} ta history candle tayyor"
    )

    return candles


# ============================================================
# BUILD HISTORICAL DATABASE
# ============================================================

async def build_history_db(
    client,
    symbol,
    interval
):

    candles = await download_6m_history(
        client,
        symbol,
        interval
    )

    db = HistoricalDB(
        symbol,
        interval
    )

    if len(candles) < 1000:

        log.warning(
            f"{symbol} [{interval}]: "
            f"history juda kam"
        )

        return db

    log.info(
        f"🧠 {symbol} [{interval}]: "
        f"historical setup'lar hisoblanmoqda..."
    )

    total = 0

    # We cannot use setups near the very end
    # because their future outcome is unknown.
    last_train_idx = (
        len(candles) - 200
    )

    for idx in range(
        10,
        last_train_idx
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

        result_r = evaluate_historical_trade(
            candles,
            idx,
            signal
        )

        if result_r is None:
            continue

        db.add(
            features,
            result_r
        )

        total += 1

        if total % 5000 == 0:

            log.info(
                f"🧠 {symbol} [{interval}]: "
                f"{total} setup..."
            )

    db.build_time_stats()

    db.ready = True

    if candles:

        db.loaded_from = candles[0]["time"]
        db.loaded_to = candles[-1]["time"]

    log.info(
        f"✅ {symbol} [{interval}]: "
        f"{len(db.rows)} historical setup "
        f"tayyor"
    )

    return db


# ============================================================
# CHART
# ============================================================

def make_chart(
    candles,
    trades,
    symbol,
    interval,
    suffix="",
    open_positions=None
):

    try:

        if not candles:
            return None

        df = pd.DataFrame(
            candles[
                -CONFIG["CHART_CANDLES"]:
            ]
        )

        df["time"] = pd.to_datetime(
            df["time"],
            unit="s"
        )

        df = df.set_index(
            "time"
        )

        fig, ax = plt.subplots(
            figsize=(12, 6),
            facecolor="#0a0b0f"
        )

        ax.set_facecolor(
            "#0a0b0f"
        )

        for i, (
            idx,
            row
        ) in enumerate(
            df.iterrows()
        ):

            c = (
                "#10b981"
                if row["close"] >= row["open"]
                else "#ef4444"
            )

            ax.plot(
                [i, i],
                [row["low"], row["high"]],
                color=c,
                linewidth=1
            )

            ax.plot(
                [i, i],
                [row["open"], row["close"]],
                color=c,
                linewidth=4
            )

        index_list = list(
            df.index
        )

        def mark(
            t,
            price,
            side,
            text
        ):

            try:

                tt = datetime.utcfromtimestamp(
                    t
                )

                if tt not in index_list:
                    return

                i = index_list.index(
                    tt
                )

                buy = side == "B"

                color = (
                    "#10b981"
                    if buy
                    else "#ef4444"
                )

                marker = (
                    "^"
                    if buy
                    else "v"
                )

                ax.scatter(
                    i,
                    price,
                    color=color,
                    marker=marker,
                    s=150,
                    zorder=6,
                    edgecolors="white",
                    linewidths=0.5
                )

                ax.annotate(
                    text,
                    xy=(i, price),
                    xytext=(
                        i,
                        price
                        -
                        (
                            df["high"].max()
                            -
                            df["low"].min()
                        ) * 0.02
                        if buy
                        else
                        price
                        +
                        (
                            df["high"].max()
                            -
                            df["low"].min()
                        ) * 0.02
                    ),
                    color=color,
                    fontsize=8,
                    fontweight="bold",
                    ha="center"
                )

            except Exception:
                pass

        # ----------------------------------------------------
        # OPEN / ENTRY MARKERS
        # ----------------------------------------------------

        for t in trades[-30:]:

            mark(
                t["time"],
                t["entry"],
                t["type"],
                f"{t.get('part','')}"
            )

        # ----------------------------------------------------
        # ADDITION 2:
        # CLOSED TRADE EXIT MARKER
        # ----------------------------------------------------

        for t in trades[-30:]:

            if (
                t.get("exit") is not None
                and
                t.get("exit_time") is not None
            ):

                mark(
                    t["exit_time"],
                    t["exit"],
                    t["type"],
                    f"{t.get('part','')} EXIT"
                )

        if open_positions:

            for p in open_positions:

                mark(
                    p["time"],
                    p["entry"],
                    p["type"],
                    f"{p.get('part','')} OPEN"
                )

        ax.set_title(
            f"{symbol} · {interval} {suffix}",
            color="#e2e8f0",
            fontsize=14
        )

        ax.tick_params(
            colors="#94a3b8"
        )

        ax.grid(
            True,
            alpha=0.1
        )

        plt.tight_layout()

        buf = io.BytesIO()

        plt.savefig(
            buf,
            format="png",
            dpi=80,
            facecolor="#0a0b0f"
        )

        plt.close(fig)

        buf.seek(0)

        return buf

    except Exception as e:

        log.error(
            f"chart: {e}"
        )

        return None


# ============================================================
# ENGINE
# ============================================================

class Engine:

    def __init__(
        self,
        symbol,
        interval,
        history_db
    ):

        self.symbol = symbol

        # ====================================================
        # ADDITION 1:
        # Every Engine is independent by symbol + timeframe.
        # ====================================================

        self.interval = interval

        self.db = history_db

        self.balance = (
            CONFIG["BALANCE"]
        )

        self.initial = (
            CONFIG["BALANCE"]
        )

        self.positions = []

        self.candles = []

        self.trades = []

        self.completedTrades = 0

        self.wins = 0
        self.losses = 0
        self.bes = 0

        self.total_comm = 0.0

        self.gross_pnl = 0.0

        self.tp1_hits = 0

        self.trail_steps = 0

        self.trail_caps = 0

        self.rt_entries = 0

        self.rt_confirmed = 0

        self.rt_rejected = 0

        self.history_pass = 0

        self.history_block = 0

        self.history_insufficient = 0

        self.time_block = 0

        self.consecutive_losses = {
            "B": 0,
            "S": 0,
        }

        self.blocked_direction = {
            "B": False,
            "S": False,
        }

        self.last_signal_time = None

        self.last_trigger_candle = None

        self.pending_signal = None

        self.day_start_balance = (
            self.balance
        )

        self.day_key = None

        self.trading_paused = False

        self.last_candle_time = None

        self.stats_2c = {
            "wins": 0,
            "losses": 0,
            "net": 0.0,
        }

    # --------------------------------------------------------
    # RISK
    # --------------------------------------------------------

    def calc_lot(
        self,
        sl_dist,
        price,
        part_risk_ratio
    ):

        if (
            sl_dist <= 0
            or price <= 0
        ):
            return 0.0

        # TOTAL signal risk = 2%
        # A/B each 1%
        risk_money = (
            self.balance
            *
            CONFIG["RISK_PCT"]
            *
            part_risk_ratio
        )

        lot = (
            risk_money /
            sl_dist
        )

        if (
            lot * sl_dist
            <
            CONFIG["MIN_RISK_USD"]
        ):

            lot = (
                CONFIG["MIN_RISK_USD"]
                /
                sl_dist
            )

        lot = max(
            CONFIG["LOT_MIN"],
            min(
                lot,
                CONFIG["LOT_MAX"]
            )
        )

        max_lot = (
            self.balance *
            0.95 /
            price
        )

        lot = min(
            lot,
            max_lot
        )

        return round(
            lot,
            6
        )

    # --------------------------------------------------------
    # DAY RESET
    # --------------------------------------------------------

    def check_day_reset(self):

        today = (
            datetime.now(timezone.utc)
            .strftime("%Y-%m-%d")
        )

        if self.day_key != today:

            self.day_key = today

            self.day_start_balance = (
                self.balance
            )

            self.trading_paused = False

    # --------------------------------------------------------
    # SELF BLOCK
    # --------------------------------------------------------

    def is_blocked(
        self,
        direction
    ):

        return self.blocked_direction.get(
            direction,
            False
        )

    def register_result(
        self,
        direction,
        net_pnl
    ):

        if net_pnl < 0:

            self.consecutive_losses[
                direction
            ] += 1

        else:

            # A non-loss breaks the streak
            self.consecutive_losses[
                direction
            ] = 0

        if (
            self.consecutive_losses[
                direction
            ]
            >=
            CONFIG["MAX_CONSECUTIVE_LOSSES"]
        ):

            self.blocked_direction[
                direction
            ] = True

    def reset_block_if_structure_changed(
        self,
        signal,
        candles=None
    ):

        # ====================================================
        # ADDITION 3:
        # Structure can now be checked against
        # the current LIVE candle.
        # ====================================================

        source_candles = (
            candles
            if candles is not None
            else self.candles
        )

        if len(source_candles) < 10:
            return

        sf = structure_features(
            source_candles[-10:]
        )

        if (
            signal["type"] == "B"
            and
            sf["trend"] < -0.50
        ):

            self.blocked_direction[
                "B"
            ] = False

            self.consecutive_losses[
                "B"
            ] = 0

        elif (
            signal["type"] == "S"
            and
            sf["trend"] > 0.50
        ):

            self.blocked_direction[
                "S"
            ] = False

            self.consecutive_losses[
                "S"
            ] = 0

    # --------------------------------------------------------
    # OPEN LOCAL
    # --------------------------------------------------------

    def open_local(
        self,
        signal,
        candle,
        part,
        entry_price=None
    ):

        entry = (
            float(entry_price)
            if entry_price is not None
            else float(candle["close"])
        )

        # Structural SL
        if signal["type"] == "B":

            base_sl = min(
                float(candle["low"]),
                float(
                    self.candles[-2]["low"]
                ) if len(self.candles) >= 2
                else float(candle["low"])
            )

            # Small buffer
            buf = (
                CONFIG["SL_BUF"]
                *
                (
                    entry /
                    100000
                )
            )

            sl = base_sl - buf

            sl_dist = (
                entry - sl
            )

        else:

            base_sl = max(
                float(candle["high"]),
                float(
                    self.candles[-2]["high"]
                ) if len(self.candles) >= 2
                else float(candle["high"])
            )

            buf = (
                CONFIG["SL_BUF"]
                *
                (
                    entry /
                    100000
                )
            )

            sl = base_sl + buf

            sl_dist = (
                sl - entry
            )

        if sl_dist <= 0:
            return None

        # A/B = 50/50 of total risk
        lot = self.calc_lot(
            sl_dist,
            entry,
            CONFIG["PART_RISK_RATIO"]
        )

        if lot <= 0:
            return None

        return {
            "time": int(candle["time"]),

            "type": signal["type"],

            "engulfCandles": signal[
                "candles"
            ],

            "entry": entry,

            "sl": sl,

            "initialSL": sl,

            "slDist": sl_dist,

            "lot": lot,

            "riskPerR": (
                lot *
                sl_dist
            ),

            "balance_at_entry": (
                self.balance
            ),

            "beSet": False,

            "tp1Done": False,

            "lockR": 0.0,

            "gross": 0.0,

            "commission": 0.0,

            "part": part,

            "is_realtime": True,

            "history_count": 0,

            "history_good_pct": 0.0,

            "history_avg_r": 0.0,

            "time_good_pct": 0.0,
        }

    # --------------------------------------------------------
    # MANAGE
    # --------------------------------------------------------

    def manage_local(
        self,
        p,
        candle
    ):

        sl_hit = False

        exit_price = 0.0

        exit_r = 0.0

        if p["type"] == "B":

            if (
                float(candle["low"])
                <=
                p["sl"]
            ):

                sl_hit = True

                exit_price = p["sl"]

                exit_r = (
                    exit_price -
                    p["entry"]
                ) / p["slDist"]

        else:

            if (
                float(candle["high"])
                >=
                p["sl"]
            ):

                sl_hit = True

                exit_price = p["sl"]

                exit_r = (
                    p["entry"] -
                    exit_price
                ) / p["slDist"]

        # ----------------------------------------------------
        # SL
        # ----------------------------------------------------

        if sl_hit:

            p["exit"] = exit_price

            p["exitR"] = exit_r

            # =================================================
            # ADDITION 2:
            # Remember exact candle time for close chart.
            # =================================================

            p["exit_time"] = int(
                candle["time"]
            )

            p["exit_candle"] = dict(
                candle
            )

            if (
                p["beSet"]
                and
                p.get("lockR", 0) == 0
            ):

                p["closeReason"] = "BE"

            elif p.get("lockR", 0) > 0:

                p["closeReason"] = (
                    f"Trail "
                    f"{p['lockR']:.1f}R"
                )

            else:

                p["closeReason"] = "SL"

            p["gross"] += (
                exit_r *
                p["riskPerR"]
            )

            return True

        # ----------------------------------------------------
        # MAX R
        # ----------------------------------------------------

        if p["type"] == "B":

            max_r = (
                float(candle["high"]) -
                p["entry"]
            ) / p["slDist"]

        else:

            max_r = (
                p["entry"] -
                float(candle["low"])
            ) / p["slDist"]

        # ----------------------------------------------------
        # BE
        # ----------------------------------------------------

        if (
            max_r >= CONFIG["BE_AT_R"]
            and
            not p["beSet"]
        ):

            p["sl"] = p["entry"]

            p["beSet"] = True

        # ----------------------------------------------------
        # A
        # ----------------------------------------------------

        if p["part"] == "A":

            if (
                max_r >=
                CONFIG["A_TP_R"]
                and
                not p["tp1Done"]
            ):

                if p["type"] == "B":

                    exit_a = (
                        p["entry"] +
                        CONFIG["A_TP_R"] *
                        p["slDist"]
                    )

                else:

                    exit_a = (
                        p["entry"] -
                        CONFIG["A_TP_R"] *
                        p["slDist"]
                    )

                p["exit"] = exit_a

                p["exitR"] = (
                    CONFIG["A_TP_R"]
                )

                # =================================================
                # ADDITION 2:
                # Remember exact candle time for close chart.
                # =================================================

                p["exit_time"] = int(
                    candle["time"]
                )

                p["exit_candle"] = dict(
                    candle
                )

                p["closeReason"] = (
                    "TP1_A_2R"
                )

                p["gross"] += (
                    CONFIG["A_TP_R"]
                    *
                    p["riskPerR"]
                )

                p["tp1Done"] = True

                self.tp1_hits += 1

                return True

            return False

        # ----------------------------------------------------
        # B
        # ----------------------------------------------------

        if p["part"] == "B":

            if (
                max_r >=
                CONFIG["MAX_TRAIL_R"]
            ):

                if p["type"] == "B":

                    exit_b = (
                        p["entry"] +
                        CONFIG["MAX_TRAIL_R"]
                        *
                        p["slDist"]
                    )

                else:

                    exit_b = (
                        p["entry"] -
                        CONFIG["MAX_TRAIL_R"]
                        *
                        p["slDist"]
                    )

                p["exit"] = exit_b

                p["exitR"] = (
                    CONFIG["MAX_TRAIL_R"]
                )

                # =================================================
                # ADDITION 2:
                # Remember exact candle time for close chart.
                # =================================================

                p["exit_time"] = int(
                    candle["time"]
                )

                p["exit_candle"] = dict(
                    candle
                )

                p["closeReason"] = (
                    f"MAX_CAP "
                    f"{CONFIG['MAX_TRAIL_R']:.0f}R"
                )

                p["gross"] += (
                    CONFIG["MAX_TRAIL_R"]
                    *
                    p["riskPerR"]
                )

                self.trail_caps += 1

                return True

            # 4R -> +2R
            # 6R -> +4R
            # 8R -> +6R
            if (
                max_r >=
                CONFIG["BE_AT_R"]
            ):

                steps = int(
                    max_r /
                    CONFIG["TRAIL_STEP_R"]
                )

                lock_r = (
                    steps - 1
                ) * CONFIG["TRAIL_STEP_R"]

                if lock_r > 0:

                    if p["type"] == "B":

                        new_sl = (
                            p["entry"] +
                            lock_r *
                            p["slDist"]
                        )

                        if new_sl > p["sl"]:

                            p["sl"] = new_sl

                            p["lockR"] = lock_r

                            self.trail_steps += 1

                    else:

                        new_sl = (
                            p["entry"] -
                            lock_r *
                            p["slDist"]
                        )

                        if new_sl < p["sl"]:

                            p["sl"] = new_sl

                            p["lockR"] = lock_r

                            self.trail_steps += 1

            return False

        return False

    # --------------------------------------------------------
    # CLOSE
    # --------------------------------------------------------

    async def close_signal(
        self,
        p
    ):

        close_comm = (
            p["lot"] *
            p["exit"] *
            CONFIG["COMM_RATE"]
        )

        p["commission"] += (
            close_comm
        )

        self.total_comm += (
            close_comm
        )

        net_pnl = (
            p["gross"] -
            p["commission"]
        )

        self.balance += (
            net_pnl
        )

        self.gross_pnl += (
            p["gross"]
        )

        p["net_pnl"] = net_pnl

        self.completedTrades += 1

        if net_pnl > 0.01:

            p["result"] = "W"

            self.wins += 1

        elif net_pnl < -0.01:

            p["result"] = "L"

            self.losses += 1

        else:

            p["result"] = "BE"

            self.bes += 1

        self.stats_2c["net"] += (
            net_pnl
        )

        if net_pnl > 0.01:

            self.stats_2c["wins"] += 1

        elif net_pnl < -0.01:

            self.stats_2c["losses"] += 1

        self.register_result(
            p["type"],
            net_pnl
        )

        self.trades.append(
            p
        )

        if p in self.positions:

            self.positions.remove(
                p
            )

        log.info(
            f"{self.symbol} "
            f"[{self.interval}] "
            f"[{p['part']}] "
            f"{p['exitR']:+.2f}R | "
            f"net ${net_pnl:+.2f} | "
            f"balance ${self.balance:.2f}"
        )

        emoji = (
            "✅"
            if p["result"] == "W"
            else
            "❌"
            if p["result"] == "L"
            else
            "⚪"
        )

        blocked = (
            " | 🛑 BLOCK"
            if self.blocked_direction[
                p["type"]
            ]
            else ""
        )

        await tg.send(
            f"{emoji} "
            f"<b>YOPILDI "
            f"[{p['part']}] "
            f"{self.symbol} "
            f"[{self.interval}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 R: "
            f"<b>{p['exitR']:+.2f}R</b>\n"
            f"📌 {p.get('closeReason','SL')}\n"
            f"💵 Gross: "
            f"${p['gross']:+.2f}\n"
            f"🔻 Comm: "
            f"-${p['commission']:.2f}\n"
            f"💰 Net: "
            f"<b>${net_pnl:+.2f}</b>\n"
            f"📈 Balance: "
            f"<b>${self.balance:.2f}</b>\n"
            f"🔥 Consecutive loss "
            f"{p['type']}: "
            f"{self.consecutive_losses[p['type']]}"
            f"{blocked}"
        )

        # ====================================================
        # ADDITION 2:
        # Send CLOSE chart with EXIT marker.
        #
        # If the exit candle is still live and not yet inside
        # self.candles, temporarily add it only for chart.
        # ====================================================

        chart_candles = list(
            self.candles
        )

        exit_candle = p.get(
            "exit_candle"
        )

        if exit_candle:

            if (
                not chart_candles
                or
                chart_candles[-1]["time"]
                !=
                exit_candle["time"]
            ):

                chart_candles.append(
                    exit_candle
                )

        ch = make_chart(
            chart_candles,
            self.trades,
            self.symbol,
            self.interval,
            f"· {p.get('part','')} EXIT",
            self.positions
        )

        if ch:

            await tg.photo(
                ch,
                f"{self.symbol} [{self.interval}] · "
                f"{p.get('part','')} EXIT · "
                f"{p.get('closeReason','SL')}"
            )

        self.check_day_reset()

        if (
            self.day_start_balance > 0
        ):

            day_loss = (
                self.day_start_balance -
                self.balance
            ) / self.day_start_balance

        else:

            day_loss = 0

        if (
            day_loss >=
            CONFIG["MAX_DAILY_LOSS_PCT"]
            and
            not self.trading_paused
        ):

            self.trading_paused = True

            await tg.send(
                f"🛑 <b>{self.symbol} "
                f"[{self.interval}]: "
                f"KUNLIK LOSS LIMIT</b>\n"
                f"📉 "
                f"-{day_loss*100:.2f}%"
            )

    # --------------------------------------------------------
    # HISTORY FILTER
    # --------------------------------------------------------

    def history_check(
        self,
        signal,
        candle
    ):

        # ====================================================
        # ADDITION 3:
        #
        # Build the exact live candle state first.
        #
        # self.candles contains CLOSED candles.
        # The current websocket candle may not yet exist there.
        #
        # History filter must therefore use:
        #
        # OLD CLOSED CANDLES + CURRENT LIVE CANDLE
        #
        # This makes the historical feature calculation
        # identical to the candle that generated the live signal.
        # ====================================================

        if (
            self.candles
            and
            self.candles[-1]["time"]
            ==
            candle["time"]
        ):

            live_candles = (
                self.candles[:-1]
                +
                [candle]
            )

        else:

            live_candles = (
                self.candles
                +
                [candle]
            )

        self.reset_block_if_structure_changed(
            signal,
            live_candles
        )

        if self.is_blocked(
            signal["type"]
        ):

            return {
                "allow": False,
                "reason": "SELF_BLOCK_10_LOSSES",
                "count": 0,
                "good_pct": 0,
                "avg_r": 0,
                "time_ok": False,
            }

        if not self.db.ready:

            return {
                "allow": False,
                "reason": "HISTORY_NOT_READY",
                "count": 0,
                "good_pct": 0,
                "avg_r": 0,
                "time_ok": False,
            }

        idx = len(
            live_candles
        ) - 1

        features = make_features(
            live_candles,
            idx,
            signal
        )

        if not features:

            return {
                "allow": False,
                "reason": "FEATURE_ERROR",
                "count": 0,
                "good_pct": 0,
                "avg_r": 0,
                "time_ok": False,
            }

        result = self.db.evaluate(
            features
        )

        return result

    # --------------------------------------------------------
    # OPEN SIGNAL
    # --------------------------------------------------------

    async def open_signal(
        self,
        signal,
        candle,
        entry_price,
        history_result,
        realtime=True
    ):

        self.check_day_reset()

        if self.trading_paused:
            return False

        # Need two positions
        slots_needed = 2

        if (
            len(self.positions) +
            slots_needed
            >
            CONFIG["MAX_OPEN_POS"]
        ):

            log.info(
                f"{self.symbol} [{self.interval}]: "
                f"MAX OPEN reached"
            )

            return False

        if (
            self.last_signal_time
            ==
            candle["time"]
        ):

            return False

        p_a = self.open_local(
            signal,
            candle,
            "A",
            entry_price
        )

        if not p_a:
            return False

        p_b = self.open_local(
            signal,
            candle,
            "B",
            entry_price
        )

        if not p_b:
            return False

        for p in (
            p_a,
            p_b
        ):

            p["history_count"] = (
                history_result.get(
                    "count",
                    0
                )
            )

            p["history_good_pct"] = (
                history_result.get(
                    "good_pct",
                    0
                )
            )

            p["history_avg_r"] = (
                history_result.get(
                    "avg_r",
                    0
                )
            )

            ts = history_result.get(
                "time_stat"
            )

            if ts:

                p["time_good_pct"] = (
                    ts.get(
                        "good_pct",
                        0
                    )
                )

            open_comm = (
                p["lot"] *
                p["entry"] *
                CONFIG["COMM_RATE"]
            )

            p["commission"] += (
                open_comm
            )

            self.total_comm += (
                open_comm
            )

        self.positions.append(
            p_a
        )

        self.positions.append(
            p_b
        )

        self.last_signal_time = (
            candle["time"]
        )

        if realtime:

            self.rt_entries += 1

        action = (
            "BUY"
            if signal["type"] == "B"
            else
            "SELL"
        )

        emoji = (
            "🟢"
            if signal["type"] == "B"
            else
            "🔴"
        )

        time_stat = (
            history_result.get(
                "time_stat"
            )
        )

        time_pct = (
            time_stat.get(
                "good_pct",
                0
            )
            if time_stat
            else 0
        )

        await tg.send(
            f"{emoji} "
            f"<b>ENTRY {action} "
            f"{self.symbol} "
            f"[{self.interval}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚡ REALTIME PRICE\n"
            f"💵 Entry: "
            f"<b>${entry_price:.6f}</b>\n"
            f"🛡 SL: "
            f"${p_a['sl']:.6f}\n"
            f"📚 6M similar: "
            f"<b>{history_result['count']}</b>\n"
            f"📈 Similar good: "
            f"<b>{history_result['good_pct']*100:.1f}%</b>\n"
            f"📊 Avg R: "
            f"<b>{history_result['avg_r']:+.2f}R</b>\n"
            f"⏰ Time good: "
            f"<b>{time_pct*100:.1f}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 A: 1% risk → +2R CLOSE\n"
            f"🎯 B: 1% risk → 2R BE\n"
            f"   4R→SL+2R\n"
            f"   6R→SL+4R\n"
            f"   8R→SL+6R\n"
            f"   10R→CLOSE\n"
            f"📂 Open: "
            f"{len(self.positions)}/"
            f"{CONFIG['MAX_OPEN_POS']}"
        )

        # ====================================================
        # ADDITION 3:
        # Entry chart uses the actual live candle too.
        # ====================================================

        chart_candles = list(
            self.candles
        )

        if (
            not chart_candles
            or
            chart_candles[-1]["time"]
            !=
            candle["time"]
        ):

            chart_candles.append(
                candle
            )

        else:

            chart_candles[-1] = candle

        ch = make_chart(
            chart_candles,
            self.trades,
            self.symbol,
            self.interval,
            f"· {action} REALTIME",
            self.positions
        )

        if ch:

            await tg.photo(
                ch,
                f"{self.symbol} [{self.interval}] · {action}"
            )

        return True

    # --------------------------------------------------------
    # REALTIME SIGNAL
    # --------------------------------------------------------

    async def handle_realtime(
        self,
        candle
    ):

        if len(
            self.candles
        ) < 10:

            return

        # Build temporary candle list
        temp = (
            self.candles[:-1]
            +
            [candle]
            if
            self.candles
            and
            self.candles[-1]["time"]
            ==
            candle["time"]
            else
            self.candles +
            [candle]
        )

        idx = len(
            temp
        ) - 1

        signal = detect_engulfing(
            temp,
            idx
        )

        if not signal:

            return

        # ----------------------------------------------------
        # IMPORTANT:
        # trigger must happen in CURRENT candle.
        # ----------------------------------------------------

        current_price = float(
            candle["close"]
        )

        trigger = float(
            signal["trigger"]
        )

        if signal["type"] == "B":

            triggered = (
                current_price >= trigger
            )

        else:

            triggered = (
                current_price <= trigger
            )

        if not triggered:

            return

        # One trigger per candle
        trigger_key = (
            f"{candle['time']}:"
            f"{signal['type']}"
        )

        if (
            self.last_trigger_candle
            ==
            trigger_key
        ):

            return

        # ----------------------------------------------------
        # Confirmation
        # ----------------------------------------------------

        if self.pending_signal is None:

            self.pending_signal = {
                "signal": signal,
                "started_at": time.time(),
                "count": 1,
                "trigger": trigger,
                "candle_time": candle[
                    "time"
                ],
            }

            log.info(
                f"⏳ {self.symbol} "
                f"[{self.interval}]: "
                f"{signal['type']} "
                f"triggered "
                f"1/{CONFIG['CONFIRM_TICKS']}"
            )

            return

        pending = (
            self.pending_signal
        )

        # Different direction
        if (
            pending["signal"]["type"]
            !=
            signal["type"]
        ):

            self.pending_signal = None

            self.rt_rejected += 1

            return

        elapsed = (
            time.time()
            -
            pending["started_at"]
        )

        # Confirm only if price still
        # respects trigger.
        if signal["type"] == "B":

            still_valid = (
                current_price >= trigger
            )

        else:

            still_valid = (
                current_price <= trigger
            )

        if not still_valid:

            self.pending_signal = None

            self.rt_rejected += 1

            return

        pending["count"] += 1

        # ----------------------------------------------------
        # Confirmation completed
        # ----------------------------------------------------

        if (
            elapsed >=
            CONFIG["CONFIRM_SECONDS"]
            and
            pending["count"]
            >=
            CONFIG["CONFIRM_TICKS"]
        ):

            self.rt_confirmed += 1

            self.pending_signal = None

            # =================================================
            # ADDITION 3:
            # History filter receives the same CURRENT
            # open candle that generated the live signal.
            # =================================================

            history_result = (
                self.history_check(
                    signal,
                    candle
                )
            )

            reason = history_result[
                "reason"
            ]

            if not history_result[
                "allow"
            ]:

                if (
                    reason ==
                    "SIMILAR_TOO_FEW"
                ):

                    self.history_insufficient += 1

                elif (
                    reason ==
                    "HISTORY_FILTER"
                ):

                    self.history_block += 1

                elif (
                    reason ==
                    "SELF_BLOCK_10_LOSSES"
                ):

                    self.history_block += 1

                log.info(
                    f"🛑 {self.symbol} "
                    f"[{self.interval}] "
                    f"{signal['type']} "
                    f"BLOCK: {reason} | "
                    f"similar="
                    f"{history_result['count']} | "
                    f"good="
                    f"{history_result['good_pct']*100:.1f}%"
                )

                return

            self.history_pass += 1

            # Entry EXACTLY at current
            # realtime price.
            entry_price = current_price

            opened = await self.open_signal(
                signal,
                candle,
                entry_price,
                history_result,
                realtime=True
            )

            if opened:

                self.last_trigger_candle = (
                    trigger_key
                )

    # --------------------------------------------------------
    # CLOSED CANDLE
    # --------------------------------------------------------

    async def handle_closed(
        self,
        candle
    ):

        if (
            self.candles
            and
            self.candles[-1]["time"]
            ==
            candle["time"]
        ):

            self.candles[-1] = candle

        else:

            self.candles.append(
                candle
            )

        if len(
            self.candles
        ) > 500:

            self.candles.pop(0)

        # If realtime is disabled,
        # use close-based entry.
        if not CONFIG[
            "REALTIME_ENTRY"
        ]:

            idx = len(
                self.candles
            ) - 1

            signal = detect_engulfing(
                self.candles,
                idx
            )

            if signal:

                history_result = (
                    self.history_check(
                        signal,
                        candle
                    )
                )

                if history_result[
                    "allow"
                ]:

                    await self.open_signal(
                        signal,
                        candle,
                        float(candle["close"]),
                        history_result,
                        realtime=False
                    )

        # New closed candle invalidates
        # unfinished realtime confirmation.
        self.pending_signal = None

    # --------------------------------------------------------
    # MANAGE POSITIONS
    # --------------------------------------------------------

    async def manage_positions(
        self,
        candle
    ):

        for p in list(
            self.positions
        ):

            try:

                closed = self.manage_local(
                    p,
                    candle
                )

                if closed:

                    await self.close_signal(
                        p
                    )

            except Exception as e:

                log.error(
                    f"{self.symbol} "
                    f"[{self.interval}] "
                    f"position manage: {e}"
                )


# ============================================================
# PRELOAD CURRENT CANDLES
# ============================================================

async def preload_current(
    client,
    engine
):

    try:

        klines = await client.get_klines(
            symbol=engine.symbol,
            interval=engine.interval,
            limit=CONFIG["PRELOAD_CANDLES"]
        )

        candles = []

        for k in klines[:-1]:

            candles.append({
                "time": int(
                    k[0] // 1000
                ),

                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "closed": True,
            })

        engine.candles = candles

        log.info(
            f"📥 {engine.symbol} "
            f"[{engine.interval}]: "
            f"{len(candles)} current candles"
        )

    except Exception as e:

        log.error(
            f"{engine.symbol} "
            f"[{engine.interval}] "
            f"preload: {e}"
        )


# ============================================================
# GLOBAL
# ============================================================

ENGINES = {}

HISTORY_DBS = {}


# ============================================================
# WORKER
# ============================================================

async def worker(
    client,
    symbol,
    interval
):

    # ========================================================
    # ADDITION 1:
    # Unique DB/Engine key = SYMBOL + TIMEFRAME
    # ========================================================

    key = (
        symbol,
        interval
    )

    db = HISTORY_DBS[
        key
    ]

    engine = Engine(
        symbol,
        interval,
        db
    )

    ENGINES[
        key
    ] = engine

    log.info(
        f"🔵 {symbol} [{interval}] worker start"
    )

    await preload_current(
        client,
        engine
    )

    bsm = BinanceSocketManager(
        client
    )

    while True:

        try:

            socket = (
                bsm.kline_socket(
                    symbol=symbol,
                    interval=interval
                )
            )

            async with socket as stream:

                log.info(
                    f"🟢 {symbol} "
                    f"[{interval}] socket connected"
                )

                while True:

                    msg = await stream.recv()

                    if (
                        not msg
                        or
                        msg.get("e")
                        !=
                        "kline"
                    ):

                        continue

                    engine.last_candle_time = (
                        time.time()
                    )

                    k = msg["k"]

                    candle = {
                        "time": int(
                            k["t"] // 1000
                        ),

                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),

                        "closed": bool(k["x"]),
                    }

                    # ------------------------------------------------
                    # REALTIME
                    # ------------------------------------------------

                    if (
                        CONFIG[
                            "REALTIME_ENTRY"
                        ]
                        and
                        not candle["closed"]
                    ):

                        await engine.handle_realtime(
                            candle
                        )

                    # ------------------------------------------------
                    # CLOSED
                    # ------------------------------------------------

                    if candle["closed"]:

                        await engine.handle_closed(
                            candle
                        )

                    # ------------------------------------------------
                    # POSITIONS
                    # ------------------------------------------------

                    await engine.manage_positions(
                        candle
                    )

        except Exception as e:

            log.error(
                f"{symbol} [{interval}] "
                f"socket error: "
                f"{e} — reconnect 5s"
            )

            await asyncio.sleep(
                5
            )


# ============================================================
# HEALTH
# ============================================================

async def health_check():

    warned = {}

    while True:

        await asyncio.sleep(
            60
        )

        now = time.time()

        for key, engine in ENGINES.items():

            symbol, interval = key

            if (
                engine.last_candle_time
                is None
            ):

                continue

            silent = (
                now -
                engine.last_candle_time
            )

            limit = (
                CONFIG[
                    "SOCKET_TIMEOUT_MIN"
                ]
                *
                60
            )

            warn_key = (
                symbol,
                interval
            )

            if (
                silent >= limit
                and
                not warned.get(warn_key)
            ):

                warned[warn_key] = True

                await tg.send(
                    f"⚠️ <b>{symbol} "
                    f"[{interval}] "
                    f"SOCKET JIM</b>\n"
                    f"{int(silent//60)} min"
                )

            elif silent < limit:

                warned[warn_key] = False


# ============================================================
# DAILY DIAGNOSTICS
# ============================================================

async def daily_diagnostics():

    sent = None

    while True:

        now = datetime.now(
            timezone.utc
        )

        key = now.strftime(
            "%Y-%m-%d"
        )

        if (
            now.hour ==
            CONFIG["DIAGNOSTICS_HOUR"]
            and
            sent != key
        ):

            sent = key

            if ENGINES:

                total_trades = sum(
                    e.completedTrades
                    for e in ENGINES.values()
                )

                total_wins = sum(
                    e.wins
                    for e in ENGINES.values()
                )

                total_losses = sum(
                    e.losses
                    for e in ENGINES.values()
                )

                total_net = sum(
                    e.balance -
                    e.initial
                    for e in ENGINES.values()
                )

                rt_confirmed = sum(
                    e.rt_confirmed
                    for e in ENGINES.values()
                )

                rt_entries = sum(
                    e.rt_entries
                    for e in ENGINES.values()
                )

                history_pass = sum(
                    e.history_pass
                    for e in ENGINES.values()
                )

                history_block = sum(
                    e.history_block
                    for e in ENGINES.values()
                )

                insufficient = sum(
                    e.history_insufficient
                    for e in ENGINES.values()
                )

                wr = (
                    total_wins /
                    (
                        total_wins +
                        total_losses
                    )
                    *
                    100
                    if
                    (
                        total_wins +
                        total_losses
                    ) > 0
                    else 0
                )

                txt = (
                    f"🔍 <b>DIAGNOSTICS</b> "
                    f"{now.strftime('%d.%m.%Y')}\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"Trades: {total_trades}\n"
                    f"WR: {wr:.1f}%\n"
                    f"Net: ${total_net:+.2f}\n\n"
                    f"⚡ RT confirmed: "
                    f"{rt_confirmed}\n"
                    f"⚡ RT opened: "
                    f"{rt_entries}\n\n"
                    f"📚 History PASS: "
                    f"{history_pass}\n"
                    f"🛑 History BLOCK: "
                    f"{history_block}\n"
                    f"📚 Similar < "
                    f"{CONFIG['MIN_SIMILAR']}: "
                    f"{insufficient}\n\n"
                )

                for key, e in ENGINES.items():

                    symbol, interval = key

                    db = e.db

                    txt += (
                        f"<b>{symbol} "
                        f"[{interval}]</b>\n"
                        f"Balance: "
                        f"${e.balance:.2f}\n"
                        f"Trades: "
                        f"{e.completedTrades}\n"
                        f"W/L: "
                        f"{e.wins}/"
                        f"{e.losses}\n"
                        f"RT: "
                        f"{e.rt_entries}\n"
                        f"Hist pass: "
                        f"{e.history_pass}\n"
                        f"Block B/S: "
                        f"{e.blocked_direction['B']}/"
                        f"{e.blocked_direction['S']}\n"
                        f"6M setups: "
                        f"{len(db.rows)}\n\n"
                    )

                await tg.send(
                    txt
                )

        await asyncio.sleep(
            60
        )


# ============================================================
# DAILY REPORT
# ============================================================

async def daily_report():

    sent = None

    while True:

        now = datetime.now(
            timezone.utc
        )

        key = now.strftime(
            "%Y-%m-%d"
        )

        if (
            now.hour ==
            CONFIG["REPORT_HOUR"]
            and
            now.minute < 1
            and
            sent != key
        ):

            sent = key

            if ENGINES:

                total_balance = sum(
                    e.balance
                    for e in ENGINES.values()
                )

                total_initial = sum(
                    e.initial
                    for e in ENGINES.values()
                )

                total_w = sum(
                    e.wins
                    for e in ENGINES.values()
                )

                total_l = sum(
                    e.losses
                    for e in ENGINES.values()
                )

                total = (
                    total_w +
                    total_l
                )

                wr = (
                    total_w /
                    total *
                    100
                    if total > 0
                    else 0
                )

                pct = (
                    (
                        total_balance -
                        total_initial
                    )
                    /
                    total_initial
                    *
                    100
                    if total_initial
                    else 0
                )

                txt = (
                    f"📊 <b>DAILY REPORT</b> "
                    f"{now.strftime('%d.%m.%Y')}\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"JAMI BALANCE: "
                    f"<b>${total_balance:.2f}</b>\n"
                    f"RESULT: "
                    f"<b>{pct:+.2f}%</b>\n"
                    f"WR: "
                    f"<b>{wr:.1f}%</b>\n\n"
                )

                for key, e in ENGINES.items():

                    symbol, interval = key

                    trades = (
                        e.wins +
                        e.losses
                    )

                    swr = (
                        e.wins /
                        trades *
                        100
                        if trades > 0
                        else 0
                    )

                    txt += (
                        f"{symbol} [{interval}]: "
                        f"${e.balance:.2f} | "
                        f"WR {swr:.1f}% | "
                        f"W{e.wins}/"
                        f"L{e.losses} | "
                        f"RT {e.rt_entries}\n"
                    )

                txt += (
                    f"\n📚 6M Similar filter: "
                    f">= "
                    f"{CONFIG['MIN_SIMILAR']} "
                    f"and >= "
                    f"{CONFIG['SIMILAR_GOOD_PCT']*100:.0f}%\n"
                    f"⏰ Time filter: >= "
                    f"{CONFIG['TIME_MIN_GOOD_PCT']*100:.0f}%\n"
                    f"🛑 Self block: "
                    f"{CONFIG['MAX_CONSECUTIVE_LOSSES']} losses\n"
                    f"⚖️ Total risk/signal: "
                    f"{CONFIG['RISK_PCT']*100:.1f}%"
                )

                await tg.send(
                    txt
                )

        await asyncio.sleep(
            30
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    log.info(
        "🚀 EngulfingTrend Bot v6.0.0"
    )

    log.info(
        f"Symbols: "
        f"{CONFIG['SYMBOLS']}"
    )

    # ========================================================
    # ADDITION 1:
    # Show all independent timeframes.
    # ========================================================

    log.info(
        f"TFs: "
        f"{CONFIG['INTERVALS']}"
    )

    log.info(
        f"6M history: "
        f"{CONFIG['HISTORY_MONTHS']} months"
    )

    log.info(
        f"Similarity minimum: "
        f"{CONFIG['MIN_SIMILAR']}"
    )

    log.info(
        f"Similarity good: "
        f"{CONFIG['SIMILAR_GOOD_PCT']*100:.1f}%"
    )

    log.info(
        f"Time filter: "
        f"{CONFIG['TIME_MIN_GOOD_PCT']*100:.1f}%"
    )

    log.info(
        f"Total risk/signal: "
        f"{CONFIG['RISK_PCT']*100:.1f}%"
    )

    log.info(
        "A = 1% / +2R"
    )

    log.info(
        "B = 1% / BE2R / "
        "4R→+2R / "
        "6R→+4R / "
        "8R→+6R / "
        "10R close"
    )

    client = await AsyncClient.create()

    try:

        # ----------------------------------------------------
        # TELEGRAM START
        # ----------------------------------------------------

        await tg.send(
            f"🚀 <b>EngulfingTrend "
            f"v6.0.0 AKTIV</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Realtime entry: "
            f"<b>{CONFIG['REALTIME_ENTRY']}</b>\n"
            f"⏱ Timeframes: "
            f"<b>{', '.join(CONFIG['INTERVALS'])}</b>\n"
            f"📚 Historical: "
            f"<b>{CONFIG['HISTORY_MONTHS']} oy</b>\n"
            f"🔎 Similar minimum: "
            f"<b>{CONFIG['MIN_SIMILAR']}</b>\n"
            f"📈 Good minimum: "
            f"<b>{CONFIG['SIMILAR_GOOD_PCT']*100:.0f}%</b>\n"
            f"⏰ Time filter: "
            f"<b>{CONFIG['TIME_MIN_GOOD_PCT']*100:.0f}%</b>\n"
            f"🛑 Loss block: "
            f"<b>{CONFIG['MAX_CONSECUTIVE_LOSSES']}</b>\n"
            f"⚖️ Total risk: "
            f"<b>{CONFIG['RISK_PCT']*100:.1f}%</b>\n"
            f"🎯 A: +2R\n"
            f"🎯 B: BE2R + trailing\n"
        )

        # ----------------------------------------------------
        # BUILD 6 MONTH DATABASES
        # ----------------------------------------------------

        for symbol in CONFIG["SYMBOLS"]:

            for interval in CONFIG["INTERVALS"]:

                try:

                    db = await build_history_db(
                        client,
                        symbol,
                        interval
                    )

                    key = (
                        symbol,
                        interval
                    )

                    HISTORY_DBS[
                        key
                    ] = db

                    await tg.send(
                        f"📚 <b>{symbol} "
                        f"[{interval}]</b>\n"
                        f"6M history tayyor\n"
                        f"Setup: "
                        f"<b>{len(db.rows)}</b>\n"
                        f"Time buckets: "
                        f"<b>{len(db.time_stats)}</b>"
                    )

                except Exception as e:

                    log.error(
                        f"{symbol} [{interval}] "
                        f"history build: "
                        f"{e}"
                    )

                    HISTORY_DBS[
                        (
                            symbol,
                            interval
                        )
                    ] = HistoricalDB(
                        symbol,
                        interval
                    )

        # ----------------------------------------------------
        # WORKERS
        # ----------------------------------------------------

        tasks = []

        for symbol in CONFIG[
            "SYMBOLS"
        ]:

            for interval in CONFIG[
                "INTERVALS"
            ]:

                key = (
                    symbol,
                    interval
                )

                if key not in HISTORY_DBS:
                    continue

                tasks.append(
                    asyncio.create_task(
                        worker(
                            client,
                            symbol,
                            interval
                        )
                    )
                )

        tasks.append(
            asyncio.create_task(
                daily_report()
            )
        )

        tasks.append(
            asyncio.create_task(
                health_check()
            )
        )

        tasks.append(
            asyncio.create_task(
                daily_diagnostics()
            )
        )

        await asyncio.gather(
            *tasks
        )

    except Exception as e:

        log.error(
            f"MAIN ERROR: {e}"
        )

        await tg.send(
            f"❌ <b>BOT ERROR</b>\n"
            f"<code>{str(e)[:1000]}</code>"
        )

    finally:

        await client.close_connection()


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
            "Bot stopped"
        )