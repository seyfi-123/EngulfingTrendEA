# EngulfingTrend Bot v7.3.0
# MARKET DATA: Twelve Data (REST API polling, 1 daqiqada bir marta)
#
# v7.2.0 -> v7.3.0:
#   1) YANGI MUSTAQIL SIGNAL: INSIDE BAR + YO'NALISHLI PROBOY
#      - 1-sham (Ona sham): katta impulsli sham
#      - 2-sham (Ichki sham): to'liq 1-sham ichida (high<=, low>=)
#      - 3-sham: 1,2-sham bilan bir xil rangda BO'LISHI SHART va
#        1-shamning High (BUY) yoki Low (SELL) darajasini yorib
#        o'tishi SHART. Aks holda (3-sham teskari rang yoki teskari
#        yo'nalishda) signal berilmaydi (taqiq).
#      Bu signal SWING va FLAT-BREAKOUT signallariga QO'SHIMCHA
#      (OR) - har biri mustaqil ishlaydi, birortasi mos kelsa signal
#      chiqadi.
#   2) POLL_INTERVAL_SEC = 60 (1 daqiqa) - API ga har 1 daqiqada
#      ulanadi.
#
# Boshqa hamma narsa v7.2.0 dan o'zgarishsiz.

import os
import io
import sys
import math
import time
import json
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
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

    "BALANCE": _f("BALANCE", 1000),
    "RISK_PCT": _f("RISK_PCT", 0.02),
    "PART_RISK_RATIO": _f("PART_RISK_RATIO", 0.50),
    "LOT_MIN": _f("LOT_MIN", 0.001),
    "LOT_MAX": _f("LOT_MAX", 5.0),
    "MAX_OPEN_POS": _i("MAX_OPEN_POS", 10),

    "MAX_SIGNALS_PER_SYMBOL": _i("MAX_SIGNALS_PER_SYMBOL", 1),

    "PRELOAD_CANDLES": _i("PRELOAD_CANDLES", 300),

    # ---- SWING STRUCTURE ----
    "SWING_LOOKBACK": _i("SWING_LOOKBACK", 2),
    "SWING_MAX_AGE_CANDLES": _i("SWING_MAX_AGE_CANDLES", 30),

    # ---- FLAT / BREAKOUT ENGULFING ----
    "FLAT_LOOKBACK": _i("FLAT_LOOKBACK", 4),
    "FLAT_MAX_RANGE_VS_AVG": _f("FLAT_MAX_RANGE_VS_AVG", 2.5),
    "BREAKOUT_MIN_BODY_RATIO": _f("BREAKOUT_MIN_BODY_RATIO", 0.55),

    "SL_BUF_FRAC": _f("SL_BUF_FRAC", 0.05),

    "SPREAD_PCT": _f("SPREAD_PCT", 0.0002),

    "REALTIME_ENTRY": os.getenv("REALTIME_ENTRY", "true").lower() == "true",
    "CONFIRM_SECONDS": _f("CONFIRM_SECONDS", 2.5),
    "CONFIRM_TICKS": _i("CONFIRM_TICKS", 2),

    "A_TP_R": _f("A_TP_R", 2),
    "BE_AT_R": _f("BE_AT_R", 2),
    "TRAIL_STEP_R": _f("TRAIL_STEP_R", 2),
    "MAX_TRAIL_R": _f("MAX_TRAIL_R", 10),

    "COMM_RATE": _f("COMM_RATE", 0.0005),

    "MAX_CONSECUTIVE_LOSSES": _i("MAX_CONSECUTIVE_LOSSES", 10),

    "REPORT_HOUR": _i("REPORT_HOUR", 18),
    "DIAGNOSTICS_HOUR": _i("DIAGNOSTICS_HOUR", 9),
    "SOCKET_TIMEOUT_MIN": _i("SOCKET_TIMEOUT_MIN", 30),
    "MAX_DAILY_LOSS_PCT": _f("MAX_DAILY_LOSS_PCT", 0.15),
    "CHART_CANDLES": _i("CHART_CANDLES", 100),
    "HISTORY_LIMIT": _i("HISTORY_LIMIT", 1000),

    "TD_REQ_PER_MIN": _i("TD_REQ_PER_MIN", 6),

    "STREAM_STALE_SEC": _i("STREAM_STALE_SEC", 300),

    # ---- REST API POLLING ----
    # API'ga har 1 daqiqada (60 sek) ulanadi
    "POLL_INTERVAL_SEC": _i("POLL_INTERVAL_SEC", 60),
    "POLL_OUTPUTSIZE": _i("POLL_OUTPUTSIZE", 5),

    "MIN_RANGE_VS_AVG": _f("MIN_RANGE_VS_AVG", 0.5),
    "MIN_BODY_RATIO": _f("MIN_BODY_RATIO", 0.3),
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

logging.getLogger("httpx").setLevel(logging.WARNING)

log = logging.getLogger("EngulfingTrend")


# ============================================================
# HELPERS
# ============================================================

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


_FX_WARNED = set()


def to_account_ccy(symbol, amount, ref_price):
    if "/" not in symbol:
        return amount
    base, quote = symbol.split("/", 1)
    if quote == "USD":
        return amount
    if base == "USD":
        if ref_price <= 0:
            return amount
        return amount / ref_price
    if symbol not in _FX_WARNED:
        _FX_WARNED.add(symbol)
        log.warning(
            "%s: USD ishtirok etmagan juft, PnL konvertatsiyasi "
            "yo'q (taxminiy)", symbol
        )
    return amount


# ============================================================
# TWELVE DATA CLIENT (faqat REST)
# ============================================================

class DailyLimitError(Exception):
    pass


class TwelveData:

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = None
        self.request_times = []
        self.lock = asyncio.Lock()
        self.max_rpm = max(1, CONFIG["TD_REQ_PER_MIN"])

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
                log.info("Rate limiter: waiting %.1f sec...", max(wait, 1))
                await asyncio.sleep(max(wait, 1))

    async def request(self, endpoint, params):
        await self.start()
        params = dict(params)
        params["apikey"] = self.api_key
        url = f"https://api.twelvedata.com/{endpoint}"

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
                log.warning("Twelve Data connection error: %s. Retry in %d sec", e, wait)
                await asyncio.sleep(wait)
                continue

            if status == 429:
                try:
                    base = float(retry_after) if retry_after else 0
                except Exception:
                    base = 0
                wait = max(base, 65) + attempt * 20
                log.warning("HTTP 429. Waiting %.0f sec", wait)
                await asyncio.sleep(wait)
                async with self.lock:
                    self.request_times = []
                continue

            if status != 200:
                raise RuntimeError(f"Twelve Data HTTP {status}: {text[:500]}")

            try:
                data = json.loads(text)
            except Exception:
                raise RuntimeError(f"Invalid response: {text[:500]}")

            if isinstance(data, dict) and data.get("status") == "error":
                message = str(data.get("message", "Twelve Data API error"))
                low = message.lower()
                if "no data" in low:
                    return {"values": []}
                if "daily" in low or "per day" in low or "for the day" in low:
                    raise DailyLimitError(message)
                if "limit" in low or "credits" in low or "too many" in low:
                    wait = 65 + attempt * 20
                    log.warning("Twelve Data limit: %s. Wait %ds", message[:120], wait)
                    await asyncio.sleep(wait)
                    async with self.lock:
                        self.request_times = []
                    continue
                raise RuntimeError(message)

            return data

        raise RuntimeError("Twelve Data request failed after 6 attempts")

    async def time_series(self, symbol, interval, outputsize=300):
        params = {
            "symbol": normalize_symbol(symbol),
            "interval": td_interval(interval),
            "outputsize": outputsize,
            "format": "JSON",
            "timezone": "UTC",
        }
        return await self.request("time_series", params)


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


def avg_range(candles):
    if not candles:
        return 1e-12
    return sum(candle_range(x) for x in candles) / max(len(candles), 1)


# ============================================================
# ENGULFING — 1 ta sham OLDINGI 2 ta shamni yutadi
# ============================================================

def detect_engulfing(candles, idx):
    if idx < 2:
        return None

    prev1 = candles[idx - 1]
    prev2 = candles[idx - 2]
    cur   = candles[idx]

    o1, c1 = float(prev1["open"]), float(prev1["close"])
    o2, c2 = float(prev2["open"]), float(prev2["close"])
    co, cc = float(cur["open"]),  float(cur["close"])

    prev_high = max(
        o1, c1, o2, c2,
        float(prev1["high"]), float(prev2["high"])
    )
    prev_low = min(
        o1, c1, o2, c2,
        float(prev1["low"]), float(prev2["low"])
    )

    is_cur_bull = cc > co
    is_cur_bear = cc < co

    both_bearish = (c1 < o1) and (c2 < o2)
    both_bullish = (c1 > o1) and (c2 > o2)

    signal = None

    if both_bearish and is_cur_bull and co <= prev_low and cc >= prev_high:
        signal = "BUY"
    elif both_bullish and is_cur_bear and co >= prev_high and cc <= prev_low:
        signal = "SELL"

    if signal is None:
        return None

    lookback = 10
    start = max(0, idx - lookback)
    context = candles[start:idx]

    if context:
        ar = avg_range(context)
        if ar > 0 and candle_range(cur) < CONFIG["MIN_RANGE_VS_AVG"] * ar:
            return None

    if body_ratio(cur) < CONFIG["MIN_BODY_RATIO"]:
        return None

    return signal


# ============================================================
# SWING STRUCTURE + FLAT-BREAKOUT + INSIDE BAR (3 mustaqil signal)
# ============================================================

class SwingTracker:

    def __init__(self):
        self.swing_highs = []
        self.swing_lows = []
        self._checked_idx = -1
        self.last_buy_swing_idx = None
        self.last_sell_swing_idx = None

    def _is_swing_high(self, candles, i, lb):
        if i - lb < 0 or i + lb >= len(candles):
            return False
        h = float(candles[i]["high"])
        for j in range(i - lb, i + lb + 1):
            if j == i:
                continue
            if float(candles[j]["high"]) >= h:
                return False
        return True

    def _is_swing_low(self, candles, i, lb):
        if i - lb < 0 or i + lb >= len(candles):
            return False
        l = float(candles[i]["low"])
        for j in range(i - lb, i + lb + 1):
            if j == i:
                continue
            if float(candles[j]["low"]) <= l:
                return False
        return True

    def update(self, candles):
        lb = CONFIG["SWING_LOOKBACK"]
        i = len(candles) - 1 - lb
        if i <= self._checked_idx or i < 0:
            return
        self._checked_idx = i

        if self._is_swing_high(candles, i, lb):
            self.swing_highs.append((i, float(candles[i]["high"])))
            if len(self.swing_highs) > 20:
                self.swing_highs.pop(0)

        if self._is_swing_low(candles, i, lb):
            self.swing_lows.append((i, float(candles[i]["low"])))
            if len(self.swing_lows) > 20:
                self.swing_lows.pop(0)

    def buy_structure_ready(self, current_idx):
        if len(self.swing_highs) < 2:
            return False, None
        h_prev, h_last = self.swing_highs[-2], self.swing_highs[-1]
        if h_last[1] <= h_prev[1]:
            return False, None
        max_age = CONFIG["SWING_MAX_AGE_CANDLES"]
        if max_age > 0 and (current_idx - h_last[0]) > max_age:
            return False, None
        if h_last[0] == self.last_buy_swing_idx:
            return False, None
        return True, h_last[0]

    def sell_structure_ready(self, current_idx):
        if len(self.swing_lows) < 2:
            return False, None
        l_prev, l_last = self.swing_lows[-2], self.swing_lows[-1]
        if l_last[1] >= l_prev[1]:
            return False, None
        max_age = CONFIG["SWING_MAX_AGE_CANDLES"]
        if max_age > 0 and (current_idx - l_last[0]) > max_age:
            return False, None
        if l_last[0] == self.last_sell_swing_idx:
            return False, None
        return True, l_last[0]

    def _check_breakout_engulfing(self, candles, idx):
        lb = CONFIG["FLAT_LOOKBACK"]
        if idx < max(lb, 2) + 1:
            return None, None

        window = candles[idx - lb:idx]
        if len(window) < lb:
            return None, None

        recent_high = max(float(c["high"]) for c in window)
        recent_low = min(float(c["low"]) for c in window)
        span = recent_high - recent_low
        if span <= 0:
            return None, None

        ar = avg_range(window)
        if ar > 0 and span > CONFIG["FLAT_MAX_RANGE_VS_AVG"] * ar:
            return None, None

        eng = detect_engulfing(candles, idx)
        if eng is None:
            return None, None

        cur = candles[idx]
        if body_ratio(cur) < CONFIG["BREAKOUT_MIN_BODY_RATIO"]:
            return None, None

        cc = float(cur["close"])

        if eng == "BUY" and cc > recent_high:
            return "BUY", {
                "type": "BREAKOUT_HIGH",
                "range_high": recent_high,
                "range_low": recent_low,
                "breakout": cc,
            }

        if eng == "SELL" and cc < recent_low:
            return "SELL", {
                "type": "BREAKOUT_LOW",
                "range_high": recent_high,
                "range_low": recent_low,
                "breakout": cc,
            }

        return None, None

    def _check_inside_bar_breakout(self, candles, idx):
        """
        YANGI (v7.3.0): Inside Bar + yo'nalishli proboy modeli.

        c1 (idx-2) - Ona sham (katta impulsli sham)
        c2 (idx-1) - Ichki sham (to'liq c1 ichida: high<=c1.high,
                     low>=c1.low)
        c3 (idx)   - Proboy shami: c1/c2 bilan BIR XIL rangda
                     bo'lishi SHART va c1'ning High (BUY) yoki
                     Low (SELL) darajasini yorib o'tishi SHART.

        Taqiq: c3 teskari rangda bo'lsa yoki teskari yo'nalishda
        harakat qilsa, signal berilmaydi.
        """

        if idx < 2:
            return None, None

        c1 = candles[idx - 2]
        c2 = candles[idx - 1]
        c3 = candles[idx]

        c1_high = float(c1["high"])
        c1_low = float(c1["low"])

        is_inside = (
            float(c2["high"]) <= c1_high and float(c2["low"]) >= c1_low
        )

        if not is_inside:
            return None, None

        c1_o, c1_c = float(c1["open"]), float(c1["close"])
        c2_o, c2_c = float(c2["open"]), float(c2["close"])
        c3_o, c3_c = float(c3["open"]), float(c3["close"])

        c1_bear = c1_c < c1_o
        c2_bear = c2_c < c2_o
        c1_bull = c1_c > c1_o
        c2_bull = c2_c > c2_o
        c3_bear = c3_c < c3_o
        c3_bull = c3_c > c3_o

        c3_low = float(c3["low"])
        c3_high = float(c3["high"])

        if c1_bear and c2_bear and c3_bear and c3_low < c1_low:
            return "SELL", {
                "type": "INSIDE_BAR_SELL",
                "mother_high": c1_high,
                "mother_low": c1_low,
                "breakout": c3_low,
            }

        if c1_bull and c2_bull and c3_bull and c3_high > c1_high:
            return "BUY", {
                "type": "INSIDE_BAR_BUY",
                "mother_high": c1_high,
                "mother_low": c1_low,
                "breakout": c3_high,
            }

        return None, None

    def check_signal(self, candles, idx):
        """
        3 ta MUSTAQIL signal manbai (OR mantiq): birortasi mos
        kelsa, shu yetarli. Ustuvorlik: SWING -> BREAKOUT ->
        INSIDE_BAR (agar bir nechtasi bir vaqtda mos kelsa, birinchi
        topilgani qaytariladi).
        """

        eng = detect_engulfing(candles, idx)

        if eng is not None:
            if eng == "BUY":
                ready, swing_idx = self.buy_structure_ready(idx)
                if ready:
                    self.last_buy_swing_idx = swing_idx
                    return "BUY", "SWING"
            elif eng == "SELL":
                ready, swing_idx = self.sell_structure_ready(idx)
                if ready:
                    self.last_sell_swing_idx = swing_idx
                    return "SELL", "SWING"

        bo_sig, bo_info = self._check_breakout_engulfing(candles, idx)
        if bo_sig is not None:
            return bo_sig, ("BREAKOUT", bo_info)

        ib_sig, ib_info = self._check_inside_bar_breakout(candles, idx)
        if ib_sig is not None:
            return ib_sig, ("INSIDE_BAR", ib_info)

        return None, None


# ============================================================
# CHART
# ============================================================

def make_chart(
    candles, symbol, interval,
    entry=None, exit_price=None, signal=None,
    entry_time=None, exit_time=None,
):

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
        bottom = min(o, cl)
        height = abs(cl - o)
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

def fill_price(price, side, entering):
    slip = price * CONFIG["SPREAD_PCT"]
    buy = side == "BUY"
    if entering:
        return price + slip if buy else price - slip
    return price - slip if buy else price + slip


def comm_cost(lot, price):
    return lot * price * CONFIG["COMM_RATE"]


def swing_info_from_meta(swings, signal, meta):
    """Telegram xabari uchun struktura ma'lumotini normallashtiradi."""

    if meta == "SWING":
        if signal == "BUY":
            h_prev, h_last = swings.swing_highs[-2], swings.swing_highs[-1]
            return {"type": "HIGH", "prev": h_prev[1], "last": h_last[1]}
        else:
            l_prev, l_last = swings.swing_lows[-2], swings.swing_lows[-1]
            return {"type": "LOW", "prev": l_prev[1], "last": l_last[1]}

    if isinstance(meta, tuple) and meta[0] == "BREAKOUT":
        bi = meta[1] or {}
        return {
            "type": bi.get("type", "BREAKOUT"),
            "prev": bi.get("range_low", 0.0),
            "last": bi.get("range_high", 0.0),
        }

    if isinstance(meta, tuple) and meta[0] == "INSIDE_BAR":
        bi = meta[1] or {}
        return {
            "type": bi.get("type", "INSIDE_BAR"),
            "prev": bi.get("mother_low", 0.0),
            "last": bi.get("mother_high", 0.0),
        }

    return {}


class Engine:

    def __init__(self, symbol, interval):
        self.symbol = normalize_symbol(symbol)
        self.interval = interval
        self.swings = SwingTracker()

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
        self.daily_paused = False

        self.last_candle_time = time.time()
        self.pending = {}

        self._sig_counter = 0

    def reset_day(self):
        today = datetime.now(timezone.utc).date()
        if today != self.day:
            self.day = today
            self.day_start_balance = self.balance
            self.consecutive_losses = 0
            self.self_blocked = False
            self.daily_paused = False

    def daily_loss_limit_hit(self):
        loss = self.day_start_balance - self.balance
        limit = self.day_start_balance * CONFIG["MAX_DAILY_LOSS_PCT"]
        return loss >= limit

    def can_trade(self):
        self.reset_day()
        if self.self_blocked:
            return False
        if self.daily_paused:
            return False
        if self.daily_loss_limit_hit():
            self.daily_paused = True
            return False
        if len(self.positions) >= CONFIG["MAX_OPEN_POS"]:
            return False
        return True

    def open_signals_for(self, signal=None):
        if signal is None:
            return len(self.positions)
        return sum(1 for p in self.positions if p["signal"] == signal)

    def has_opposite(self, signal):
        opposite = "SELL" if signal == "BUY" else "BUY"
        return any(p["signal"] == opposite for p in self.positions)

    def calc_lot(self, entry, sl, part_risk_ratio=1.0):
        risk_money = self.balance * CONFIG["RISK_PCT"] * part_risk_ratio
        sl_dist = abs(entry - sl)
        if sl_dist <= 0:
            return 0.0, False
        lot = risk_money / sl_dist
        capped = lot > CONFIG["LOT_MAX"] or lot < CONFIG["LOT_MIN"]
        lot = max(CONFIG["LOT_MIN"], min(lot, CONFIG["LOT_MAX"]))
        return lot, capped

    def open_local(self, signal, entry, sl, opened_at=None, swing_info=None):
        risk = (entry - sl) if signal == "BUY" else (sl - entry)
        if risk <= 0:
            return None

        lot_a, capped_a = self.calc_lot(entry, sl, CONFIG["PART_RISK_RATIO"])
        lot_b, capped_b = self.calc_lot(entry, sl, CONFIG["PART_RISK_RATIO"])

        if lot_a <= 0:
            return None

        tp_a = (
            entry + risk * CONFIG["A_TP_R"] if signal == "BUY"
            else entry - risk * CONFIG["A_TP_R"]
        )

        self._sig_counter += 1

        p = {
            "id": f"{self.symbol}-{self.interval}-{int(time.time() * 1000)}",
            "sig_id": self._sig_counter,
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
            "a_closed": False,
            "b_sl": sl,
            "commission": 0.0,
            "gross": 0.0,
            "opened": opened_at or datetime.now(timezone.utc),
            "trail_r": 0.0,
            "lot_capped": capped_a or capped_b,
            "swing_info": swing_info or {},
            "signal_net": 0.0,
            "signal_legs_closed": 0,
        }

        open_comm = comm_cost(p["lot"], p["entry"])
        p["commission"] += open_comm
        self.total_comm += open_comm

        self.positions.append(p)
        return p

    async def _send_close_report(self, p, exit_price, reason,
                                 net_pnl, gross, fraction, r_multiple,
                                 exit_time, signal_final_r=None):

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

        si = p.get("swing_info") or {}

        txt = (
            f"{emoji} <b>YOPILDI [{part_label}] {p['symbol']} "
            f"[{p['interval']}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📌 Sabab: {reason_text}\n"
            f"🎯 R (shu oyoq): <b>{r_multiple:+.2f}R</b>\n"
            f"💵 Entry: {p['entry']:.6f}\n"
            f"🚪 Exit: {exit_price:.6f}\n"
            f"💵 Gross: ${gross:+.2f}\n"
            f"🔻 Comm: -${p['commission']:.2f}\n"
            f"💰 Net (shu yopilish): <b>${net_pnl:+.2f}</b>\n"
            f"📈 Balance: <b>${self.balance:.2f}</b>"
        )

        if signal_final_r is not None:
            txt += (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"🧾 <b>SIGNAL YAKUNI (A+B):</b> "
                f"{signal_final_r:+.2f}R\n"
                f"🔥 Ketma-ket zarar (signal bo'yicha): "
                f"{self.consecutive_losses}"
            )

        if si:
            st = si.get('type', '')
            if st.startswith("BREAKOUT"):
                txt += (
                    f"\n📐 Struktura: <b>{st}</b> "
                    f"(flat {si.get('prev', 0):.4f}–{si.get('last', 0):.4f})"
                )
            elif st.startswith("INSIDE_BAR"):
                txt += (
                    f"\n📐 Struktura: <b>{st}</b> "
                    f"(ona sham {si.get('prev', 0):.4f}–{si.get('last', 0):.4f})"
                )
            else:
                txt += (
                    f"\n📐 Struktura: swing {st} "
                    f"{si.get('prev', 0):.4f} → {si.get('last', 0):.4f}"
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

    def close_signal(self, p, exit_price_raw, reason, fraction=1.0):
        if p not in self.positions:
            return

        lot = p["lot"] * fraction
        exit_price = fill_price(exit_price_raw, p["signal"], entering=False)

        if p["signal"] == "BUY":
            gross_quote = (exit_price - p["entry"]) * lot
        else:
            gross_quote = (p["entry"] - exit_price) * lot

        close_comm = comm_cost(lot, exit_price)

        gross_usd = to_account_ccy(p["symbol"], gross_quote, exit_price)
        comm_usd = to_account_ccy(p["symbol"], close_comm, exit_price)

        p["commission"] += comm_usd
        self.total_comm += comm_usd

        net_pnl = gross_usd - comm_usd

        self.balance += net_pnl
        self.gross_pnl += gross_usd
        p["gross"] += gross_usd

        exit_time = datetime.now(timezone.utc)

        risk_usd_leg = to_account_ccy(p["symbol"], p["risk"] * lot, exit_price)
        r_multiple = net_pnl / risk_usd_leg if risk_usd_leg > 0 else 0.0

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

        p["signal_net"] += net_pnl
        p["signal_legs_closed"] += 1

        full_close = fraction >= 0.999
        signal_final_r = None

        if full_close:
            total_risk_usd = to_account_ccy(
                p["symbol"], p["risk"] * (p["lot_a"] + p["lot_b"]),
                exit_price,
            )
            signal_final_r = (
                p["signal_net"] / total_risk_usd if total_risk_usd > 0 else 0.0
            )
            if p["signal_net"] > 0:
                self.stats["wins"] += 1
                self.consecutive_losses = 0
            elif p["signal_net"] < 0:
                self.stats["losses"] += 1
                self.consecutive_losses += 1

        newly_blocked = False
        if full_close and self.consecutive_losses >= CONFIG["MAX_CONSECUTIVE_LOSSES"]:
            if not self.self_blocked:
                newly_blocked = True
            self.self_blocked = True

        asyncio.create_task(
            self._send_close_report(
                p, exit_price, reason, net_pnl, gross_usd,
                fraction, r_multiple, exit_time,
                signal_final_r=signal_final_r,
            )
        )

        if newly_blocked:
            asyncio.create_task(
                TG_CLIENT.send(
                    f"🛑 <b>SELF BLOCK</b>\n"
                    f"{self.symbol} {self.interval}\n"
                    f"{CONFIG['MAX_CONSECUTIVE_LOSSES']} consecutive "
                    f"LOSING SIGNALS."
                )
            )

        if full_close:
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

        if not p["a_closed"] and tp_hit:
            old_lot = p["lot"]
            if old_lot > 0:
                self.close_signal(p, p["tp_a"], "TP_A", fraction=p["lot_a"] / old_lot)
                p["a_closed"] = True
                if p in self.positions:
                    p["lot"] -= p["lot_a"]
                    p["lot_a"] = 0
                    p["b_sl"] = entry

        if p in self.positions and r_now >= CONFIG["BE_AT_R"]:
            p["b_sl"] = max(p["b_sl"], entry) if is_buy else min(p["b_sl"], entry)

        if p in self.positions and r_now >= CONFIG["TRAIL_STEP_R"]:
            trail_r = min(
                math.floor(r_now / CONFIG["TRAIL_STEP_R"]) * CONFIG["TRAIL_STEP_R"],
                CONFIG["MAX_TRAIL_R"]
            )
            if trail_r > p["trail_r"]:
                p["trail_r"] = trail_r
                shift = risk * max(0, trail_r - CONFIG["TRAIL_STEP_R"])
                if is_buy:
                    p["b_sl"] = max(p["b_sl"], entry + shift)
                else:
                    p["b_sl"] = min(p["b_sl"], entry - shift)

        if p in self.positions:
            stop_hit = price <= p["b_sl"] if is_buy else price >= p["b_sl"]
            if stop_hit:
                self.close_signal(p, p["b_sl"], "SL_BE_TRAIL")

    def manage_positions(self, price):
        for p in list(self.positions):
            self.manage_local(p, price)

    async def open_signal(self, signal, candle, swing_info):
        if not self.can_trade():
            self.stats["blocked"] += 1
            return

        if self.has_opposite(signal):
            log.info(
                "%s %s %s: RAD ETILDI - qarshi yo'nalish ochiq",
                self.symbol, self.interval, signal
            )
            return

        if self.open_signals_for(signal) >= CONFIG["MAX_SIGNALS_PER_SYMBOL"]:
            log.info(
                "%s %s %s: RAD ETILDI - limit (%d) dan oshdi",
                self.symbol, self.interval, signal,
                CONFIG["MAX_SIGNALS_PER_SYMBOL"]
            )
            return

        self.stats["signals"] += 1

        entry_raw = float(candle["close"])
        rng = candle_range(candle)
        buf = rng * CONFIG["SL_BUF_FRAC"]

        if signal == "BUY":
            sl = float(candle["low"]) - buf
        else:
            sl = float(candle["high"]) + buf

        entry_fill = fill_price(entry_raw, signal, entering=True)

        p = self.open_local(
            signal, entry_fill, sl,
            opened_at=candle["timestamp"],
            swing_info=swing_info,
        )
        if not p:
            return

        cap_note = " ⚠️ (lot LOT_MIN/LOT_MAX bilan cheklandi)" if p["lot_capped"] else ""

        si_txt = ""
        if swing_info:
            st = swing_info.get("type", "")
            if st.startswith("BREAKOUT"):
                si_txt = (
                    f"📐 Struktura: <b>{st}</b>\n"
                    f"Flat zona: {swing_info.get('prev', 0):.4f} → "
                    f"{swing_info.get('last', 0):.4f}\n"
                )
            elif st.startswith("INSIDE_BAR"):
                si_txt = (
                    f"📐 Struktura: <b>{st}</b>\n"
                    f"Ona sham: {swing_info.get('prev', 0):.4f} → "
                    f"{swing_info.get('last', 0):.4f}\n"
                )
            else:
                si_txt = (
                    f"📐 Struktura: {st} "
                    f"{swing_info.get('prev', 0):.4f} → "
                    f"{swing_info.get('last', 0):.4f}\n"
                )

        msg = (
            f"🟢 <b>{signal}</b>\n"
            f"<b>{self.symbol}</b> | {self.interval}\n"
            f"{si_txt}"
            f"Entry: {entry_fill:.6f}\n"
            f"SL: {sl:.6f}\n"
            f"TP A: {p['tp_a']:.6f}\n"
            f"Lot: {p['lot']:.4f}{cap_note}\n"
            f"Balance: {self.balance:.2f}"
        )

        await TG_CLIENT.send(msg)

        chart = make_chart(
            self.candles, self.symbol, self.interval,
            entry=entry_fill, signal=signal, entry_time=candle["timestamp"],
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

        if len(self.candles) < 3:
            return

        self.swings.update(self.candles)

        idx = len(self.candles) - 1

        signal, meta = self.swings.check_signal(self.candles, idx)

        if signal:
            swing_info = swing_info_from_meta(self.swings, signal, meta)
            await self.open_signal(signal, candle, swing_info)

        max_keep = max(CONFIG["HISTORY_LIMIT"], CONFIG["PRELOAD_CANDLES"])
        if len(self.candles) > max_keep:
            trim = len(self.candles) - max_keep
            self.candles = self.candles[-max_keep:]
            self.swings.swing_highs = [
                (i - trim, v) for i, v in self.swings.swing_highs if i - trim >= 0
            ]
            self.swings.swing_lows = [
                (i - trim, v) for i, v in self.swings.swing_lows if i - trim >= 0
            ]
            self.swings._checked_idx -= trim
            if self.swings.last_buy_swing_idx is not None:
                self.swings.last_buy_swing_idx -= trim
            if self.swings.last_sell_swing_idx is not None:
                self.swings.last_sell_swing_idx -= trim

    async def handle_realtime(self, candle):
        self.last_candle_time = time.time()

        price = float(candle["close"])
        self.manage_positions(price)

        if not CONFIG["REALTIME_ENTRY"]:
            return

        if len(self.candles) < 3:
            return

        test_candles = list(self.candles)
        if test_candles and test_candles[-1]["timestamp"] == candle["timestamp"]:
            test_candles[-1] = candle
        else:
            test_candles.append(candle)

        if len(test_candles) < 3:
            return

        idx = len(test_candles) - 1

        eng = detect_engulfing(test_candles, idx)

        classic_ok = False
        if eng == "BUY":
            classic_ok, _ = self.swings.buy_structure_ready(idx)
        elif eng == "SELL":
            classic_ok, _ = self.swings.sell_structure_ready(idx)

        bo_sig, _ = self.swings._check_breakout_engulfing(test_candles, idx)
        ib_sig, _ = self.swings._check_inside_bar_breakout(test_candles, idx)

        candidate = None
        if eng and classic_ok:
            candidate = eng
        elif bo_sig is not None:
            candidate = bo_sig
        elif ib_sig is not None:
            candidate = ib_sig

        if candidate is None:
            return

        key = candidate
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
            candle_key = f"{candle['timestamp']}-{candidate}"
            if self.pending.get("executed") == candle_key:
                return
            self.pending["executed"] = candle_key

            old = self.candles
            self.candles = test_candles

            signal, meta = self.swings.check_signal(self.candles, idx)

            try:
                if signal:
                    swing_info = swing_info_from_meta(self.swings, signal, meta)
                    await self.open_signal(signal, candle, swing_info)
            finally:
                self.candles = old


# ============================================================
# GLOBAL ENGINES
# ============================================================

ENGINES = {}


async def preload_current(client, engine):
    try:
        data = await client.time_series(
            symbol=engine.symbol,
            interval=engine.interval,
            outputsize=CONFIG["PRELOAD_CANDLES"] + 2,
        )
        values = data.get("values", [])

        candles = []
        for row in values:
            try:
                dt = pd.to_datetime(row["datetime"], utc=True)
                candles.append({
                    "timestamp": dt.to_pydatetime(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "closed": True,
                })
            except Exception:
                continue

        candles.sort(key=lambda x: x["timestamp"])

        if candles:
            engine.candles = candles[-CONFIG["PRELOAD_CANDLES"]:]
            engine.last_candle_time = time.time()

            for i in range(len(engine.candles)):
                engine.swings.update(engine.candles[:i + 1])

            log.info(
                "Preload %s %s: %d candles, swings H=%d L=%d",
                engine.symbol, engine.interval, len(engine.candles),
                len(engine.swings.swing_highs), len(engine.swings.swing_lows)
            )

    except Exception as e:
        log.error("Preload error %s %s: %s", engine.symbol, engine.interval, e)


# ============================================================
# LIVE CANDLE BUILDER (REST polling uchun)
# ============================================================

LIVE_CANDLES = {}


def candle_bucket(timestamp, interval):
    sec = interval_seconds(interval)
    ts = int(timestamp.timestamp())
    bucket = (ts // sec) * sec
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


# ============================================================
# REST API POLLER
# ============================================================

_LAST_TICK_TS = {"t": time.time()}


async def poll_engine(client, engine):
    try:
        data = await client.time_series(
            symbol=engine.symbol,
            interval=engine.interval,
            outputsize=CONFIG["POLL_OUTPUTSIZE"],
        )
    except DailyLimitError:
        raise
    except Exception as e:
        log.error("Poll xato %s %s: %s", engine.symbol, engine.interval, e)
        return

    values = data.get("values", [])
    if not values:
        return

    fetched = []
    for row in values:
        try:
            dt = pd.to_datetime(row["datetime"], utc=True)
            fetched.append({
                "timestamp": dt.to_pydatetime(),
                "open":  float(row["open"]),
                "high":  float(row["high"]),
                "low":   float(row["low"]),
                "close": float(row["close"]),
                "closed": False,
            })
        except Exception:
            continue

    if not fetched:
        return

    fetched.sort(key=lambda x: x["timestamp"])

    now = datetime.now(timezone.utc)
    current_bucket = candle_bucket(now, engine.interval)

    for c in fetched:
        c["closed"] = (c["timestamp"] != current_bucket)

    last_engine_ts = engine.candles[-1]["timestamp"] if engine.candles else None

    for c in fetched:
        if not c["closed"]:
            continue
        if last_engine_ts is None or c["timestamp"] > last_engine_ts:
            await engine.handle_closed(c)
            last_engine_ts = c["timestamp"]

    forming = fetched[-1]
    if not forming["closed"]:
        await engine.handle_realtime(forming)


async def market_poller(client):
    log.info(
        "Market data rejimi: REST API polling (har ~%d sek, rate=%d rpm)",
        CONFIG["POLL_INTERVAL_SEC"], CONFIG["TD_REQ_PER_MIN"],
    )
    _LAST_TICK_TS["t"] = time.time()

    while True:
        cycle_start = time.time()
        ok_count = 0
        err_count = 0

        for key, engine in list(ENGINES.items()):
            try:
                await poll_engine(client, engine)
                ok_count += 1
            except DailyLimitError as e:
                log.error("Kunlik limit tugadi: %s", e)
                await TG_CLIENT.send(
                    f"⚠️ <b>Twelve Data kunlik limit</b>\n<code>{str(e)[:300]}</code>"
                )
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                err_count += 1
                log.error("Poll xato %s %s: %s", engine.symbol, engine.interval, e)

        if ok_count > 0:
            _LAST_TICK_TS["t"] = time.time()

        if err_count == 0 and ok_count > 0:
            log.info(
                "Poll sikl tugadi: %d engine yangilandi (%.1fs)",
                ok_count, time.time() - cycle_start,
            )

        elapsed = time.time() - cycle_start
        wait = max(1.0, CONFIG["POLL_INTERVAL_SEC"] - elapsed)
        await asyncio.sleep(wait)


# ============================================================
# REPORT / HEALTH / DIAGNOSTICS
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
            f"Balance {engine.balance:.2f} | PnL {pnl:.2f} | "
            f"W {engine.stats['wins']} | L {engine.stats['losses']}"
        )

    lines.extend([
        "", f"Total balance: {total_balance:.2f}",
        f"Total PnL: {total_pnl:.2f}", f"Trades: {total_trades}",
    ])
    await TG_CLIENT.send("\n".join(lines))


_ALERT_SENT = False


async def health_check():
    global _ALERT_SENT
    while True:
        try:
            now = time.time()
            silent = now - _LAST_TICK_TS["t"]

            if silent > CONFIG["STREAM_STALE_SEC"] and not _ALERT_SENT:
                _ALERT_SENT = True
                await TG_CLIENT.send(
                    "⚠️ <b>MARKET DATA JIM</b>\n"
                    f"{silent / 60:.1f} daqiqadan beri yangi REST javob yo'q.\n"
                    "🔄 Polling davom etmoqda..."
                )

            if silent < CONFIG["STREAM_STALE_SEC"] and _ALERT_SENT:
                _ALERT_SENT = False
                await TG_CLIENT.send("✅ <b>Market data oqimi tiklandi</b>")

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
                        f"swings H={len(engine.swings.swing_highs)} "
                        f"L={len(engine.swings.swing_lows)}, "
                        f"positions={len(engine.positions)}, "
                        f"signals={engine.stats['signals']}, "
                        f"blocked={engine.self_blocked}, "
                        f"daily_paused={engine.daily_paused}"
                    )
                await TG_CLIENT.send("\n".join(lines))
        except Exception as e:
            log.error("Diagnostics error: %s", e)
        await asyncio.sleep(30)


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
    log.info("EngulfingTrend Bot v7.3.0 (REST API polling, 1 daqiqa)")
    log.info("Symbols: %s", CONFIG["SYMBOLS"])
    log.info("Intervals: %s", CONFIG["INTERVALS"])
    log.info("Poll interval: %ds | Rate: %d rpm",
             CONFIG["POLL_INTERVAL_SEC"], CONFIG["TD_REQ_PER_MIN"])
    log.info("=" * 50)

    client = TwelveData(TWELVE_DATA_API_KEY)
    _LAST_TICK_TS["t"] = time.time()

    try:
        await client.start()

        symbols = [normalize_symbol(s) for s in CONFIG["SYMBOLS"]]
        intervals = CONFIG["INTERVALS"]

        for symbol in symbols:
            for interval in intervals:
                engine = Engine(symbol, interval)
                ENGINES[(symbol, interval)] = engine
                await preload_current(client, engine)

        await TG_CLIENT.send(
            "🟢 <b>EngulfingTrend Bot v7.3.0 STARTED</b>\n\n"
            "Market: Twelve Data (<b>REST API polling</b>)\n"
            f"Poll interval: {CONFIG['POLL_INTERVAL_SEC']}s (1 daqiqa)\n"
            f"Rate limit: {CONFIG['TD_REQ_PER_MIN']} req/min\n"
            f"Symbols: {', '.join(CONFIG['SYMBOLS'])}\n"
            f"Timeframes: {', '.join(CONFIG['INTERVALS'])}\n"
            f"Engulfing: <b>1 sham OLDINGI 2 tasini yutadi</b>\n"
            f"Signal 1: 2 swing high → BUY, 2 swing low → SELL\n"
            f"Signal 2: Flat + breakout impuls engulfing\n"
            f"Signal 3: Inside Bar + yo'nalishli proboy\n"
            f"Commission: {CONFIG['COMM_RATE']} | Spread: {CONFIG['SPREAD_PCT']}\n"
            f"Max signals/symbol: {CONFIG['MAX_SIGNALS_PER_SYMBOL']}"
        )

        tasks = [
            asyncio.create_task(market_poller(client)),
            asyncio.create_task(health_check()),
            asyncio.create_task(diagnostics_loop()),
            asyncio.create_task(report_loop()),
        ]

        await asyncio.gather(*tasks)

    finally:
        await client.close()
        log.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.exception("FATAL ERROR: %s", e)