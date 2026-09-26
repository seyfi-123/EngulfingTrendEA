# ============================================================
# EngulfingTrend Bot v5.9.3 — DUAL ENTRY + BODY ENGULFING + CONFIRMATION
# A: 1:1 da 100% yopiladi | B: har 2R trailing (4R->SL2, 6R->SL4, 8R->SL6, 10R->SL8), max 10R cap
# ============================================================
import os
import asyncio
import logging
import time
import io
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from telegram import Bot, InputFile
from telegram.constants import ParseMode
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

load_dotenv()

# ============================================================
# SOZLAMALAR
# ============================================================
CONFIG = {
    'SYMBOLS':    [s.strip().upper() for s in os.getenv('SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT').split(',') if s.strip()],
    'INTERVAL':   os.getenv('INTERVAL', '1m'),
    'BALANCE':    float(os.getenv('BALANCE', '1000')),
    'RISK_PCT':   float(os.getenv('RISK_PCT', '0.02')),
    'LOT_MIN':    float(os.getenv('LOT_MIN', '0.001')),
    'LOT_MAX':    float(os.getenv('LOT_MAX', '2.0')),
    'MIN_RISK_USD': float(os.getenv('MIN_RISK_USD', '2.0')),
    'MAX_OPEN_POS': int(os.getenv('MAX_OPEN_POS', '10')),
    'PRELOAD_CANDLES': int(os.getenv('PRELOAD_CANDLES', '100')),
    'SL_BUF':     float(os.getenv('SL_BUF', '10')),

    'DUAL_ENTRY': os.getenv('DUAL_ENTRY', 'True') == 'True',
    'REALTIME_ENTRY': os.getenv('REALTIME_ENTRY', 'True') == 'True',
    'CONFIRM_SECONDS': float(os.getenv('CONFIRM_SECONDS', '2.5')),

    # === Pozitsiya A ===
    'BE_AT_R':    float(os.getenv('BE_AT_R', '1.0')),
    'TP1_AT_R':   float(os.getenv('TP1_AT_R', '1.0')),      # A: 1:1 da 100% yopiladi

    # === Pozitsiya B ===
    'TRAIL_STEP': float(os.getenv('TRAIL_STEP', '2.0')),    # har 2R qadamda: 4->SL2, 6->SL4, 8->SL6, 10->SL8
    'MAX_TRAIL_R': float(os.getenv('MAX_TRAIL_R', '10.0')), # 10R ga yetganda pozitsiya yopiladi (xavfsizlik)

    'COMM_RATE':  float(os.getenv('COMM_RATE', '0.0005')),

    'CHART_CANDLES': int(os.getenv('CHART_CANDLES', '100')),
    'REPORT_HOUR':   int(os.getenv('REPORT_HOUR', '18')),
    'SOCKET_TIMEOUT_MIN':  int(os.getenv('SOCKET_TIMEOUT_MIN', '30')),
    'MAX_DAILY_LOSS_PCT':  float(os.getenv('MAX_DAILY_LOSS_PCT', '0.15')),
    'DIAGNOSTICS_HOUR':    int(os.getenv('DIAGNOSTICS_HOUR', '9')),
}

TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
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
        if not self.bot: return
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.error(f"TG: {e}")

    async def photo(self, buf, caption=""):
        if not self.bot: return
        try:
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=InputFile(buf, filename='chart.png'),
                caption=caption, parse_mode=ParseMode.HTML
            )
        except Exception as e:
            log.error(f"TG photo: {e}")

tg = TG(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)


# ============================================================
# GRAFIK
# ============================================================
def make_chart(candles, trades, symbol, interval, suffix="", open_positions=None):
    try:
        if not candles: return None
        df = pd.DataFrame(candles[-CONFIG['CHART_CANDLES']:])
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df = df.set_index('time')

        fig, ax = plt.subplots(figsize=(12, 6), facecolor='#0a0b0f')
        ax.set_facecolor('#0a0b0f')

        for i, (idx, row) in enumerate(df.iterrows()):
            c = '#10b981' if row['close'] >= row['open'] else '#ef4444'
            ax.plot([i, i], [row['low'], row['high']], color=c, linewidth=1)
            ax.plot([i, i], [row['open'], row['close']], color=c, linewidth=4)

        index_list = list(df.index)

        def mark_point(t, price, side, extra=""):
            try:
                tt = datetime.utcfromtimestamp(t)
                if tt in index_list:
                    i = index_list.index(tt)
                else: return
                is_buy = side == 'B'
                color = '#10b981' if is_buy else '#ef4444'
                marker = '^' if is_buy else 'v'
                text = ("BUY" if is_buy else "SELL") + (f" {extra}" if extra else "")
                y_off = (df['high'].max() - df['low'].min()) * 0.02
                y = price - y_off if is_buy else price + y_off
                ax.scatter(i, price, color=color, marker=marker, s=160, zorder=6, edgecolors='white', linewidths=0.5)
                ax.annotate(text, xy=(i, price), xytext=(i, y), color=color,
                            fontsize=8, fontweight='bold', ha='center',
                            va='top' if is_buy else 'bottom', zorder=7)
            except: pass

        for t in trades[-30:]:
            mark_point(t['time'], t['entry'], t['type'], f"{t.get('engulfCandles','')}C-{t.get('part','')}")
        if open_positions:
            for p in open_positions:
                mark_point(p['time'], p['entry'], p['type'], f"{p.get('engulfCandles','')}C-{p.get('part','')} (ochiq)")

        ax.set_title(f'{symbol} · {interval} {suffix}', color='#e2e8f0', fontsize=14, pad=15)
        ax.tick_params(colors='#94a3b8', labelsize=9)
        ax.grid(True, alpha=0.1, color='#ffffff')
        for sp in ax.spines.values(): sp.set_color('#ffffff14')

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=80, facecolor='#0a0b0f')
        plt.close(fig); buf.seek(0)
        return buf
    except Exception as e:
        log.error(f"chart: {e}"); return None


# ============================================================
# TRADE ENGINE
# ============================================================
class Engine:
    def __init__(self, symbol):
        self.symbol = symbol
        self.balance = CONFIG['BALANCE']
        self.initial = CONFIG['BALANCE']
        self.completedTrades = 0
        self.wins = 0; self.losses = 0; self.bes = 0
        self.tp1_hits = 0; self.trail_steps = 0; self.trail_caps = 0
        self.rt_entries = 0
        self.rt_confirmed = 0
        self.rt_rejected = 0
        self.positions = []
        self.candles = []
        self.trades = []
        self.total_comm = 0.0
        self.gross_pnl = 0.0

        self.last_candle_time = None
        self.stats_2c = {'wins': 0, 'losses': 0, 'net': 0.0}
        self.day_start_balance = CONFIG['BALANCE']
        self.day_key = None
        self.trading_paused = False
        self.last_signal_time = None

        self.pending_signal = None

    # ----------------------------------------------------------
    def calcLot(self, slDist, price):
        if slDist <= 0 or price <= 0:
            return CONFIG['LOT_MIN']
        risk = self.balance * CONFIG['RISK_PCT']
        lot = risk / slDist
        if lot * slDist < CONFIG['MIN_RISK_USD']:
            lot = CONFIG['MIN_RISK_USD'] / slDist
        lot = max(CONFIG['LOT_MIN'], min(lot, CONFIG['LOT_MAX']))
        max_lot = (self.balance * 0.95) / price
        lot = min(lot, max_lot)
        return round(lot, 4)

    # ----------------------------------------------------------
    def checkEngulfing(self, cd, idx):
        """2-candle engulfing — FAQAT BODY (open/close), fitil (high/low) hisobga olinmaydi."""
        if idx < 2: return None
        cur = cd[idx]; p1 = cd[idx-1]; p2 = cd[idx-2]

        p1_top = max(p1['open'], p1['close']); p1_bot = min(p1['open'], p1['close'])
        p2_top = max(p2['open'], p2['close']); p2_bot = min(p2['open'], p2['close'])
        body_top_2 = max(p1_top, p2_top)
        body_bot_2 = min(p1_bot, p2_bot)

        bull2 = (
            cur['close'] > cur['open']
            and p1['close'] < p1['open']
            and p2['close'] < p2['open']
            and cur['open']  < body_bot_2
            and cur['close'] > body_top_2
        )
        bear2 = (
            cur['close'] < cur['open']
            and p1['close'] > p1['open']
            and p2['close'] > p2['open']
            and cur['open']  > body_top_2
            and cur['close'] < body_bot_2
        )
        if bull2: return {'type': 'B', 'candles': 2}
        if bear2: return {'type': 'S', 'candles': 2}
        return None

    # ----------------------------------------------------------
    def openLocal(self, signal, candle, part='A'):
        cur = candle
        buf = CONFIG['SL_BUF'] * (cur['close'] / 100000)

        if signal['type'] == 'B':
            slPrice = cur['low'] - buf
            slDist = cur['close'] - slPrice
        else:
            slPrice = cur['high'] + buf
            slDist = slPrice - cur['close']

        if slDist <= 0: return None

        lot = self.calcLot(slDist, cur['close'])
        if lot <= 0: return None

        return {
            'time': cur['time'],
            'type': signal['type'],
            'engulfCandles': signal['candles'],
            'entry': cur['close'],
            'sl': slPrice,
            'initialSL': slPrice,
            'slDist': slDist,
            'lot': lot,
            'riskPerR': lot * slDist,
            'balance_at_entry': self.balance,
            'beSet': False,
            'tp1Done': False,
            'lockR': 0,
            'gross': 0.0,
            'commission': 0.0,
            'part': part,
            'is_realtime': False,
        }

    # ----------------------------------------------------------
    def manageLocal(self, p, candle):
        """Har bir pozitsiyani (A yoki B) alohida boshqaradi."""
        sl_hit = False; exit_price = 0; exitR = 0
        if p['type'] == 'B' and candle['low'] <= p['sl']:
            sl_hit = True; exit_price = p['sl']
            exitR = (exit_price - p['entry']) / p['slDist']
        elif p['type'] == 'S' and candle['high'] >= p['sl']:
            sl_hit = True; exit_price = p['sl']
            exitR = (p['entry'] - exit_price) / p['slDist']

        if sl_hit:
            p['exit'] = exit_price
            p['exitR'] = exitR
            if p['beSet'] and p.get('lockR', 0) == 0:
                p['closeReason'] = 'BE'
            elif p.get('lockR', 0) > 0:
                p['closeReason'] = f"Trail {p['lockR']:.1f}R"
            else:
                p['closeReason'] = 'SL'
            p['gross'] += exitR * p['riskPerR']
            return True

        if p['type'] == 'B':
            maxR = (candle['high'] - p['entry']) / p['slDist']
        else:
            maxR = (p['entry'] - candle['low']) / p['slDist']

        # BE @ 1:1 — ikkala pozitsiya uchun ham
        if maxR >= CONFIG['BE_AT_R'] and not p['beSet']:
            p['sl'] = p['entry']
            p['beSet'] = True

        # ============ POZITSIYA A — 1:1 da 100% yopiladi ============
        if p.get('part') == 'A':
            if maxR >= CONFIG['TP1_AT_R'] and not p.get('tp1Done'):
                if p['type'] == 'B':
                    exit_a = p['entry'] + CONFIG['TP1_AT_R'] * p['slDist']
                else:
                    exit_a = p['entry'] - CONFIG['TP1_AT_R'] * p['slDist']

                p['exit'] = exit_a
                p['exitR'] = CONFIG['TP1_AT_R']
                p['closeReason'] = 'TP1_A (1:1)'
                p['gross'] += CONFIG['TP1_AT_R'] * p['riskPerR']
                p['tp1Done'] = True
                self.tp1_hits += 1
                return True
            return False

        # ============ POZITSIYA B — har 2R trailing: 4->SL2, 6->SL4, 8->SL6, 10->SL8 ============
        if p.get('part') == 'B':
            # 10R ga yetsa — pozitsiya avtomatik yopiladi (xavfsizlik "tomi")
            if maxR >= CONFIG['MAX_TRAIL_R']:
                if p['type'] == 'B':
                    exit_b = p['entry'] + CONFIG['MAX_TRAIL_R'] * p['slDist']
                else:
                    exit_b = p['entry'] - CONFIG['MAX_TRAIL_R'] * p['slDist']
                p['exit'] = exit_b
                p['exitR'] = CONFIG['MAX_TRAIL_R']
                p['closeReason'] = f"MAX_CAP {CONFIG['MAX_TRAIL_R']:.0f}R"
                p['gross'] += CONFIG['MAX_TRAIL_R'] * p['riskPerR']
                self.trail_caps += 1
                return True

            # Har TRAIL_STEP (2R) qadamda: maxR=4->lockR=2, maxR=6->lockR=4, maxR=8->lockR=6, maxR=10->lockR=8
            if maxR >= CONFIG['BE_AT_R']:
                steps = int(maxR / CONFIG['TRAIL_STEP'])
                lockR = (steps - 1) * CONFIG['TRAIL_STEP']
                if lockR > 0:
                    if p['type'] == 'B':
                        newSL = p['entry'] + lockR * p['slDist']
                        if newSL > p['sl']:
                            p['sl'] = newSL
                            p['lockR'] = lockR
                            self.trail_steps += 1
                    else:
                        newSL = p['entry'] - lockR * p['slDist']
                        if newSL < p['sl']:
                            p['sl'] = newSL
                            p['lockR'] = lockR
                            self.trail_steps += 1
            return False

        return False

    # ----------------------------------------------------------
    def checkDayReset(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.day_key != today:
            self.day_key = today
            self.day_start_balance = self.balance
            self.trading_paused = False

    # ----------------------------------------------------------
    async def openSignal(self, signal, candle, is_realtime=False):
        """Har signal → 2 ta pozitsiya (A + B), agar DUAL_ENTRY yoqilgan bo'lsa."""
        self.checkDayReset()

        if self.trading_paused:
            return

        slots_needed = 2 if CONFIG['DUAL_ENTRY'] else 1
        if len(self.positions) + slots_needed > CONFIG['MAX_OPEN_POS']:
            log.info(f"{self.symbol}: joy yo'q ({len(self.positions)}/{CONFIG['MAX_OPEN_POS']})")
            return

        if self.last_signal_time == candle['time']:
            return

        # ============ POZITSIYA A ============
        p_a = self.openLocal(signal, candle, part='A')
        if not p_a: return
        p_a['is_realtime'] = is_realtime

        open_comm_a = p_a['lot'] * p_a['entry'] * CONFIG['COMM_RATE']
        p_a['commission'] += open_comm_a
        self.total_comm += open_comm_a

        self.positions.append(p_a)
        self.last_signal_time = candle['time']
        if is_realtime:
            self.rt_entries += 1

        # ============ POZITSIYA B ============
        if CONFIG['DUAL_ENTRY']:
            p_b = self.openLocal(signal, candle, part='B')
            if p_b:
                p_b['is_realtime'] = is_realtime
                open_comm_b = p_b['lot'] * p_b['entry'] * CONFIG['COMM_RATE']
                p_b['commission'] += open_comm_b
                self.total_comm += open_comm_b
                self.positions.append(p_b)

        mode = "⚡ RT (tasdiqlangan)" if is_realtime else "📊 Sham yopilgan"
        log.info(f"{mode} SIGNAL {self.symbol} {signal['type']} | A=1:1, B=trail(2R qadam/10R cap) | ochiq: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}")

        action = "BUY" if signal['type'] == 'B' else "SELL"
        emoji = "🟢" if signal['type'] == 'B' else "🔴"
        await tg.send(
            f"{emoji} <b>SIGNAL: {action} {self.symbol}</b> 📡\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>{mode}</b>\n"
            f"📊 Narx: <b>${p_a['entry']:.4f}</b>\n"
            f"🛡 SL: ${p_a['sl']:.4f}\n"
            f"💰 Lot (har biri): {p_a['lot']}\n"
            f"🎯 <b>2 ta pozitsiya:</b>\n"
            f"   ├─ A: TP1 @ 1:1 (100%)\n"
            f"   └─ B: 4R→SL2, 6R→SL4, 8R→SL6, 10R→SL8(yopiladi)\n"
            f"📂 Ochiq: <b>{len(self.positions)}/{CONFIG['MAX_OPEN_POS']}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

        ch = make_chart(self.candles, self.trades, self.symbol,
                        CONFIG['INTERVAL'], f"· {action}",
                        open_positions=self.positions)
        if ch:
            await tg.photo(ch, f"📊 {self.symbol} · {action}")

    # ----------------------------------------------------------
    async def closeSignal(self, p):
        close_comm = p['lot'] * p['exit'] * CONFIG['COMM_RATE']
        p['commission'] += close_comm
        self.total_comm += close_comm

        net_pnl = p['gross'] - p['commission']
        self.balance += net_pnl
        self.gross_pnl += p['gross']
        p['net_pnl'] = net_pnl
        self.completedTrades += 1

        if net_pnl > 0.01: p['result'] = 'W'; self.wins += 1
        elif net_pnl < -0.01: p['result'] = 'L'; self.losses += 1
        else: p['result'] = 'BE'; self.bes += 1

        self.stats_2c['net'] += net_pnl
        if net_pnl > 0.01: self.stats_2c['wins'] += 1
        elif net_pnl < -0.01: self.stats_2c['losses'] += 1

        self.trades.append(p)
        self.positions.remove(p)

        part = p.get('part', '?')
        log.info(f"{self.symbol} [{part}]: {p['exitR']:+.2f}R | net ${net_pnl:+.2f} | balans ${self.balance:.2f} | ochiq: {len(self.positions)}")

        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        rt_info = "⚡" if p.get('is_realtime') else "📊"
        await tg.send(
            f"{emoji} <b>Yopildi [{part}]: {self.symbol}</b> {rt_info}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Exit: ${p['exit']:.4f}\n"
            f"🎯 R: <b>{p['exitR']:+.2f}R</b>\n"
            f"📌 Sabab: <b>{p.get('closeReason', 'SL')}</b>\n"
            f"💵 Gross: ${p['gross']:+.2f}\n"
            f"🔻 Komissiya: -${p['commission']:.2f}\n"
            f"💰 <b>Net: ${net_pnl:+.2f}</b>\n"
            f"📈 Balans: <b>${self.balance:.2f}</b>\n"
            f"📂 Ochiq: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}"
        )

        ch = make_chart(self.candles, self.trades, self.symbol,
                        CONFIG['INTERVAL'], f"· yopildi [{part}]",
                        open_positions=self.positions)
        if ch:
            await tg.photo(ch, f"📊 {self.symbol} · yopildi [{part}] ({p['result']})")

        self.checkDayReset()
        day_loss = (self.day_start_balance - self.balance) / self.day_start_balance if self.day_start_balance > 0 else 0
        if day_loss >= CONFIG['MAX_DAILY_LOSS_PCT'] and not self.trading_paused:
            self.trading_paused = True
            await tg.send(f"🛑 <b>{self.symbol}: KUNLIK ZARAR LIMITI</b>\n📉 -{day_loss*100:.1f}%")

    # ============================================================
    # REALTIME + CONFIRMATION mantiqi
    # ============================================================
    async def handleRealtimeCandle(self, client, candle):
        if len(self.candles) < 2:
            return

        temp = [self.candles[-2], self.candles[-1], candle]
        sig = self.checkEngulfing(temp, 2)

        if self.pending_signal is None:
            if sig:
                self.pending_signal = {
                    'signal': sig,
                    'started_at': time.time(),
                    'last_candle': candle,
                }
                log.info(f"⏳ {self.symbol}: {sig['type']} signal kutishga qo'yildi (tasdiqlash {CONFIG['CONFIRM_SECONDS']}s)")
            return

        pending = self.pending_signal
        elapsed = time.time() - pending['started_at']

        if not sig or sig['type'] != pending['signal']['type']:
            log.info(f"❌ {self.symbol}: kutishdagi signal bekor qilindi (candle o'zgardi)")
            self.rt_rejected += 1
            self.pending_signal = None
            return

        pending['last_candle'] = candle

        if elapsed >= CONFIG['CONFIRM_SECONDS']:
            log.info(f"✅ {self.symbol}: {sig['type']} signal tasdiqlandi ({elapsed:.1f}s) — savdo ochilmoqda")
            self.rt_confirmed += 1
            self.pending_signal = None
            await self.openSignal(sig, candle, is_realtime=True)


# ============================================================
# GLOBAL
# ============================================================
ENGINES = {}


async def preload_candles(client, eng, symbol):
    try:
        klines = await client.get_klines(symbol=symbol, interval=CONFIG['INTERVAL'], limit=CONFIG['PRELOAD_CANDLES'])
        candles = []
        for k in klines[:-1]:
            candles.append({
                'time': k[0] // 1000, 'open': float(k[1]),
                'high': float(k[2]), 'low': float(k[3]),
                'close': float(k[4]), 'closed': True,
            })
        eng.candles = candles
        log.info(f"📥 {symbol}: {len(candles)} ta sham")
    except Exception as e:
        log.error(f"{symbol} preload xato: {e}")


# ============================================================
# WORKER
# ============================================================
async def worker(client, symbol):
    eng = Engine(symbol)
    ENGINES[symbol] = eng
    log.info(f"🔵 {symbol} worker boshlandi (MAX {CONFIG['MAX_OPEN_POS']}, DUAL={CONFIG['DUAL_ENTRY']}, REALTIME={CONFIG['REALTIME_ENTRY']}, CONFIRM={CONFIG['CONFIRM_SECONDS']}s)")
    await preload_candles(client, eng, symbol)

    bsm = BinanceSocketManager(client)
    while True:
        try:
            socket = bsm.kline_socket(symbol=symbol, interval=CONFIG['INTERVAL'])
            async with socket as stream:
                log.info(f"🟢 {symbol} socket ulandi")
                while True:
                    msg = await stream.recv()
                    if not msg or msg.get('e') != 'kline': continue
                    eng.last_candle_time = time.time()
                    k = msg['k']
                    candle = {
                        'time': k['t'] // 1000,
                        'open': float(k['o']), 'high': float(k['h']),
                        'low': float(k['l']), 'close': float(k['c']),
                        'closed': k['x'],
                    }

                    if CONFIG['REALTIME_ENTRY'] and not candle['closed']:
                        await eng.handleRealtimeCandle(client, candle)

                    if candle['closed']:
                        if eng.candles and eng.candles[-1]['time'] == candle['time']:
                            eng.candles[-1] = candle
                        else:
                            eng.candles.append(candle)
                        if len(eng.candles) > 200: eng.candles.pop(0)

                        eng.pending_signal = None

                        if not CONFIG['REALTIME_ENTRY']:
                            idx = len(eng.candles) - 1
                            sig = eng.checkEngulfing(eng.candles, idx)
                            if sig:
                                await eng.openSignal(sig, candle, is_realtime=False)

                    for p in list(eng.positions):
                        if eng.manageLocal(p, candle):
                            await eng.closeSignal(p)

        except Exception as e:
            log.error(f"{symbol} socket xato: {e} — 5s")
            await asyncio.sleep(5)


async def health_check():
    warned = {}
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for sym, eng in ENGINES.items():
            if eng.last_candle_time is None: continue
            silent = now - eng.last_candle_time
            if silent >= CONFIG['SOCKET_TIMEOUT_MIN'] * 60 and not warned.get(sym):
                warned[sym] = True
                await tg.send(f"⚠️ <b>{sym} socket jim!</b> {int(silent//60)} daqiqa")
            elif silent < CONFIG['SOCKET_TIMEOUT_MIN'] * 60:
                warned[sym] = False


async def daily_diagnostics():
    sent = None
    while True:
        now = datetime.now(timezone.utc)
        key = now.strftime('%Y-%m-%d')
        if now.hour == CONFIG['DIAGNOSTICS_HOUR'] and sent != key:
            sent = key
            if ENGINES:
                tw = sum(e.stats_2c['wins'] for e in ENGINES.values())
                tl = sum(e.stats_2c['losses'] for e in ENGINES.values())
                tn = sum(e.stats_2c['net'] for e in ENGINES.values())
                rt_c = sum(e.rt_confirmed for e in ENGINES.values())
                rt_r = sum(e.rt_rejected for e in ENGINES.values())
                tp1 = sum(e.tp1_hits for e in ENGINES.values())
                cap = sum(e.trail_caps for e in ENGINES.values())
                wr = tw / (tw+tl) * 100 if (tw+tl) > 0 else 0
                txt = f"🔍 <b>DIAGNOSTIKA</b> {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━\n"
                txt += f"2C: ✅{tw} ❌{tl} (WR {wr:.1f}%) | ${tn:+.2f}\n"
                txt += f"⚡ Tasdiqlangan: {rt_c} | ❌ Rad etilgan: {rt_r}\n"
                txt += f"🎯 TP1 (A): {tp1} | 🧢 MAX_CAP (B, 10R): {cap}\n\n"
                for s, e in ENGINES.items():
                    net = e.balance - e.initial
                    em = "🟢" if net >= 0 else "🔴"
                    txt += f"{em} {s}: {net:+.2f}$ | Ochiq: {len(e.positions)}\n"
                await tg.send(txt)
        await asyncio.sleep(60)


async def daily_report():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == CONFIG['REPORT_HOUR'] and now.minute < 1:
            tb = sum(e.balance for e in ENGINES.values())
            ti = sum(e.initial for e in ENGINES.values())
            tw = sum(e.wins for e in ENGINES.values())
            tl = sum(e.losses for e in ENGINES.values())
            td = sum(e.wins+e.losses+e.bes for e in ENGINES.values())
            rt_c = sum(e.rt_confirmed for e in ENGINES.values())
            wr = tw/td*100 if td > 0 else 0
            pct = (tb-ti)/ti*100 if ti else 0
            txt = f"📊 <b>KUNLIK HISOBOT</b> {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━\n"
            txt += f"⚡ Tasdiqlangan real-time: {rt_c} ta\n\n"
            for s, e in ENGINES.items():
                sp = (e.balance-e.initial)/e.initial*100
                em = "🟢" if e.balance >= e.initial else "🔴"
                sw = e.wins/(e.wins+e.losses)*100 if (e.wins+e.losses) > 0 else 0
                txt += f"\n{em} <b>{s}</b>: ${e.balance:.2f} ({sp:+.2f}%) WR:{sw:.1f}% ✅{e.wins}❌{e.losses}\n"
            txt += f"\n━━━━━━━━━━━━━━━━━━\nJAMI: WR {wr:.1f}% | ${tb:.2f} ({pct:+.2f}%)"
            await tg.send(txt)
            await asyncio.sleep(60)
        await asyncio.sleep(30)


# ============================================================
# ASOSIY
# ============================================================
async def main():
    log.info("🚀 Bot v5.9.3 — DUAL ENTRY + BODY ENGULFING + CONFIRMATION")
    log.info(f"Symbols: {CONFIG['SYMBOLS']} | TF: {CONFIG['INTERVAL']}")
    log.info(f"REALTIME={CONFIG['REALTIME_ENTRY']} | DUAL={CONFIG['DUAL_ENTRY']} | CONFIRM={CONFIG['CONFIRM_SECONDS']}s")

    rt_status = "Yoqilgan" if CONFIG['REALTIME_ENTRY'] else "Ochirilgan"
    dual_status = "Yoqilgan" if CONFIG['DUAL_ENTRY'] else "Ochirilgan"

    await tg.send(
        f"🚀 <b>Engulfing Bot v5.9.3 — DUAL ENTRY</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>REALTIME</b>: {rt_status} ({CONFIG['CONFIRM_SECONDS']}s tasdiqlash)\n"
        f"🎯 <b>DUAL ENTRY</b>: {dual_status}\n"
        f"📊 {', '.join(CONFIG['SYMBOLS'])}\n"
        f"⏱ TF: {CONFIG['INTERVAL']}\n"
        f"🎯 2C Engulfing — FAQAT BODY (open/close)\n"
        f"🎯 <b>Har signal 2 pozitsiya:</b>\n"
        f"   ├─ A: TP1 @ 1:1 (100%)\n"
        f"   └─ B: 4R→SL2, 6R→SL4, 8R→SL6, 10R→SL8(cap)\n"
        f"📂 Max pozitsiya: <b>{CONFIG['MAX_OPEN_POS']}</b>\n"
        f"⚖️ Risk: {CONFIG['RISK_PCT']*100:.1f}%\n"
        f"✅ Aktiv"
    )

    client = await AsyncClient.create()
    tasks = [asyncio.create_task(worker(client, s)) for s in CONFIG['SYMBOLS']]
    tasks.append(asyncio.create_task(daily_report()))
    tasks.append(asyncio.create_task(health_check()))
    tasks.append(asyncio.create_task(daily_diagnostics()))

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        log.error(f"Main: {e}")
    finally:
        await client.close_connection()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot to'xtatildi")