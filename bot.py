# ============================================================
# EngulfingTrend Bot v5.7.0 — SIGNAL-ONLY + BE@1:1 + TP1 30%@1:1.5
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
# SOZLAMALAR — BARCHASI RAILWAY VARIABLES ORQALI
# ============================================================
CONFIG = {
    # === Asosiy ===
    'SYMBOLS':    [s.strip().upper() for s in os.getenv('SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT').split(',') if s.strip()],
    'INTERVAL':   os.getenv('INTERVAL', '15m'),

    # === Balans va Lot ===
    'BALANCE':    float(os.getenv('BALANCE', '1000')),
    'RISK_PCT':   float(os.getenv('RISK_PCT', '0.02')),
    'LOT_MIN':    float(os.getenv('LOT_MIN', '0.001')),
    'LOT_MAX':    float(os.getenv('LOT_MAX', '2.0')),
    'MIN_RISK_USD': float(os.getenv('MIN_RISK_USD', '2.0')),  # min $2 risk/R

    # === Strategiya (Railway'dan) ===
    'SL_BUF':     float(os.getenv('SL_BUF', '10')),
    'BE_AT_R':    float(os.getenv('BE_AT_R', '1.0')),      # BE @ 1:1
    'TP1_AT_R':   float(os.getenv('TP1_AT_R', '1.5')),     # TP1 @ 1:1.5
    'TP1_PCT':    float(os.getenv('TP1_PCT', '0.30')),     # 30% yopish
    'TRAIL_STEP': float(os.getenv('TRAIL_STEP', '0.5')),   # 0.5R qadam

    # === Xavfsizlik ===
    'MAX_OPEN_POS':    int(os.getenv('MAX_OPEN_POS', '5')),
    'PRELOAD_CANDLES': int(os.getenv('PRELOAD_CANDLES', '100')),
    'COMM_RATE':       float(os.getenv('COMM_RATE', '0.0005')),
    'MAX_DAILY_LOSS_PCT':  float(os.getenv('MAX_DAILY_LOSS_PCT', '0.15')),
    'SOCKET_TIMEOUT_MIN':  int(os.getenv('SOCKET_TIMEOUT_MIN', '30')),

    # === Hisobot ===
    'CHART_CANDLES': int(os.getenv('CHART_CANDLES', '100')),
    'REPORT_HOUR':   int(os.getenv('REPORT_HOUR', '18')),
    'DIAGNOSTICS_HOUR': int(os.getenv('DIAGNOSTICS_HOUR', '9')),
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
# TELEGRAM HELPER
# ============================================================
class TG:
    def __init__(self, token, chat_id):
        self.bot = Bot(token=token) if token else None
        self.chat_id = chat_id

    async def send(self, msg):
        if not self.bot: return
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=msg,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            log.error(f"TG text: {e}")

    async def photo(self, buf, caption=""):
        if not self.bot: return
        try:
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=InputFile(buf, filename='chart.png'),
                caption=caption,
                parse_mode=ParseMode.HTML
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

        def mark_point(entry_time, entry_price, side, label_extra=""):
            try:
                tt = datetime.utcfromtimestamp(entry_time)
                if tt in index_list:
                    i = index_list.index(tt)
                else:
                    return
                is_buy = (side == 'B')
                color = '#10b981' if is_buy else '#ef4444'
                marker = '^' if is_buy else 'v'
                text = ("BUY" if is_buy else "SELL") + (f" {label_extra}" if label_extra else "")
                y_offset = (df['high'].max() - df['low'].min()) * 0.02
                y_pos = entry_price - y_offset if is_buy else entry_price + y_offset

                ax.scatter(i, entry_price, color=color, marker=marker, s=160, zorder=6, edgecolors='white', linewidths=0.5)
                ax.annotate(
                    text,
                    xy=(i, entry_price),
                    xytext=(i, y_pos),
                    color=color,
                    fontsize=8,
                    fontweight='bold',
                    ha='center',
                    va='top' if is_buy else 'bottom',
                    zorder=7
                )
            except Exception:
                pass

        for t in trades[-30:]:
            mark_point(t['time'], t['entry'], t['type'], label_extra=f"{t.get('engulfCandles','')}C")

        if open_positions:
            for p in open_positions:
                mark_point(p['time'], p['entry'], p['type'], label_extra=f"{p.get('engulfCandles','')}C (ochiq)")

        ax.set_title(f'{symbol} · {interval} {suffix}',
                     color='#e2e8f0', fontsize=14, pad=15)
        ax.tick_params(colors='#94a3b8', labelsize=9)
        ax.grid(True, alpha=0.1, color='#ffffff')
        for sp in ax.spines.values():
            sp.set_color('#ffffff14')

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=80, facecolor='#0a0b0f')
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as e:
        log.error(f"chart: {e}")
        return None


# ============================================================
# TRADE ENGINE (SIGNAL-ONLY)
# ============================================================
class Engine:
    def __init__(self, symbol):
        self.symbol = symbol
        self.balance = CONFIG['BALANCE']
        self.initial = CONFIG['BALANCE']
        self.completedTrades = 0
        self.wins = 0
        self.losses = 0
        self.bes = 0
        self.tp1_hits = 0
        self.trail_steps = 0
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

    # ----------------------------------------------------------
    def calcLot(self, slDist, price):
        """Lot = risk_usd / slDist, min $2 risk bilan"""
        if slDist <= 0 or price <= 0:
            return CONFIG['LOT_MIN']

        risk_amount = self.balance * CONFIG['RISK_PCT']
        lot = risk_amount / slDist

        # Minimum risk filter
        if lot * slDist < CONFIG['MIN_RISK_USD']:
            lot = CONFIG['MIN_RISK_USD'] / slDist

        lot = max(CONFIG['LOT_MIN'], min(lot, CONFIG['LOT_MAX']))
        max_lot_by_balance = (self.balance * 0.95) / price
        lot = min(lot, max_lot_by_balance)

        return round(lot, 4)

    # ----------------------------------------------------------
    def checkEngulfing(self, cd, idx):
        """FAQAT 2-Candle engulfing"""
        if idx < 2: return None
        cur = cd[idx]; p1 = cd[idx - 1]; p2 = cd[idx - 2]

        maxHigh2 = max(p1['high'], p2['high'])
        minLow2  = min(p1['low'],  p2['low'])

        bull2 = (cur['close'] > cur['open']
                 and p1['close'] < p1['open']
                 and p2['close'] < p2['open']
                 and cur['high'] > maxHigh2)

        bear2 = (cur['close'] < cur['open']
                 and p1['close'] > p1['open']
                 and p2['close'] > p2['open']
                 and cur['low'] < minLow2)

        if bull2: return {'type': 'B', 'candles': 2}
        if bear2: return {'type': 'S', 'candles': 2}
        return None

    # ----------------------------------------------------------
    def openLocal(self, signal, candle):
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
            'qty_remaining': lot,
            'riskPerR': lot * slDist,
            'balance_at_entry': self.balance,
            'beSet': False,
            'tp1Done': False,
            'lockR': 0,
            'gross': 0.0,
            'commission': 0.0,
            'tp1Net': 0.0,
        }

    # ----------------------------------------------------------
    def manageLocal(self, p, candle):
        """Qaytaradi: True agar to'liq yopilsa"""
        # ============================================
        # 1. SL tegilganini tekshirish (BE yoki SL)
        # ============================================
        sl_hit = False
        exit_price = 0
        exitR = 0

        if p['type'] == 'B' and candle['low'] <= p['sl']:
            sl_hit = True
            exit_price = p['sl']
            exitR = (exit_price - p['entry']) / p['slDist']
        elif p['type'] == 'S' and candle['high'] >= p['sl']:
            sl_hit = True
            exit_price = p['sl']
            exitR = (p['entry'] - exit_price) / p['slDist']

        if sl_hit:
            p['exit'] = exit_price
            p['exitR'] = exitR
            p['closeReason'] = 'BE' if p['beSet'] else 'SL'
            # Qolgan qism uchun P&L
            remaining_gross = exitR * p['riskPerR'] * (p['qty_remaining'] / p['lot'])
            p['gross'] += remaining_gross
            p['pnl'] = p['gross']  # keyin komissiya ayiriladi
            return True

        # ============================================
        # 2. Max R hisoblash
        # ============================================
        if p['type'] == 'B':
            maxR = (candle['high'] - p['entry']) / p['slDist']
        else:
            maxR = (p['entry'] - candle['low']) / p['slDist']

        # ============================================
        # 3. QOIDA 1: BE @ BE_AT_R (Railway'dan)
        # ============================================
        if maxR >= CONFIG['BE_AT_R'] and not p['beSet']:
            p['sl'] = p['entry']
            p['beSet'] = True

        # ============================================
        # 4. QOIDA 2: TP1 @ TP1_AT_R (Railway'dan) — qismiy yopish
        # ============================================
        if maxR >= CONFIG['TP1_AT_R'] and not p['tp1Done']:
            tp1_qty = p['lot'] * CONFIG['TP1_PCT']

            # TP1 exit narxi
            if p['type'] == 'B':
                tp1_price = p['entry'] + CONFIG['TP1_AT_R'] * p['slDist']
            else:
                tp1_price = p['entry'] - CONFIG['TP1_AT_R'] * p['slDist']

            # TP1 R
            tp1_r = CONFIG['TP1_AT_R']

            # TP1 gross va komissiya
            tp1_gross = tp1_r * p['riskPerR'] * CONFIG['TP1_PCT']
            tp1_comm = tp1_qty * tp1_price * CONFIG['COMM_RATE']

            p['gross'] += tp1_gross
            p['commission'] += tp1_comm
            p['tp1Net'] = tp1_gross - tp1_comm
            p['qty_remaining'] -= tp1_qty
            p['tp1Done'] = True
            self.tp1_hits += 1

            # Balansga qo'shish
            self.balance += p['tp1Net']
            self.total_comm += tp1_comm

        # ============================================
        # 5. QOIDA 3: Step Trail (TRAIL_STEP qadam)
        # ============================================
        if p['tp1Done'] and maxR >= CONFIG['TP1_AT_R']:
            steps = int(maxR / CONFIG['TRAIL_STEP'])
            lockR = (steps - 2) * CONFIG['TRAIL_STEP']

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

    # ----------------------------------------------------------
    def checkDayReset(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.day_key != today:
            self.day_key = today
            self.day_start_balance = self.balance
            if self.trading_paused:
                log.info(f"{self.symbol}: yangi kun, savdo qayta yoqildi")
            self.trading_paused = False

    # ----------------------------------------------------------
    async def openSignal(self, signal, candle):
        self.checkDayReset()

        if self.trading_paused:
            return

        if len(self.positions) >= CONFIG['MAX_OPEN_POS']:
            return

        p = self.openLocal(signal, candle)
        if not p: return

        # Ochilish komissiyasi
        open_comm = p['lot'] * p['entry'] * CONFIG['COMM_RATE']
        p['commission'] += open_comm
        self.balance -= open_comm
        self.total_comm += open_comm

        self.positions.append(p)
        log.info(f"📡 SIGNAL {self.symbol} {signal['type']} @ ${p['entry']:.4f} | lot={p['lot']} | risk/R=${p['riskPerR']:.2f}")

        action = "BUY" if signal['type'] == 'B' else "SELL"
        emoji = "🟢" if signal['type'] == 'B' else "🔴"
        await tg.send(
            f"{emoji} <b>SIGNAL: {action} {self.symbol}</b> 📡\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Narx: <b>${p['entry']:.4f}</b>\n"
            f"🛡 (virtual) SL: ${p['sl']:.4f}\n"
            f"💰 (virtual) Lot: {p['lot']} (risk/R: ${p['riskPerR']:.2f})\n"
            f"🎯 Engulf: <b>{p['engulfCandles']}C</b>\n"
            f"🎯 Rejim: BE@1:{CONFIG['BE_AT_R']} + TP1 {int(CONFIG['TP1_PCT']*100)}%@1:{CONFIG['TP1_AT_R']}\n"
            f"📂 Ochiq: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}\n"
            f"⚠️ <i>Signal-only — real order yo'q</i>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

        ch = make_chart(self.candles, self.trades, self.symbol,
                        CONFIG['INTERVAL'], f"· {action}",
                        open_positions=self.positions)
        if ch:
            await tg.photo(ch, f"📊 {self.symbol} · {action}")

    # ----------------------------------------------------------
    async def closeSignal(self, p):
        # Yopilish komissiyasi
        close_comm = p['qty_remaining'] * p['exit'] * CONFIG['COMM_RATE']
        p['commission'] += close_comm
        self.total_comm += close_comm

        net_pnl = p['gross'] - p['commission']

        self.balance += net_pnl
        self.gross_pnl += p['gross']

        p['net_pnl'] = net_pnl
        self.completedTrades += 1

        if net_pnl > 0.01:
            p['result'] = 'W'; self.wins += 1
        elif net_pnl < -0.01:
            p['result'] = 'L'; self.losses += 1
        else:
            p['result'] = 'BE'; self.bes += 1

        self.stats_2c['net'] += net_pnl
        if net_pnl > 0.01:
            self.stats_2c['wins'] += 1
        elif net_pnl < -0.01:
            self.stats_2c['losses'] += 1

        self.trades.append(p)
        self.positions.remove(p)

        log.info(f"{self.symbol}: {p['exitR']:+.2f}R | net ${net_pnl:+.2f} | balans ${self.balance:.2f}")

        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        tp1_info = f"TP1: +${p['tp1Net']:.2f}\n" if p['tp1Done'] else ""
        await tg.send(
            f"{emoji} <b>SIGNAL yopildi: {self.symbol}</b> 📡\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Exit: ${p['exit']:.4f}\n"
            f"🎯 R: <b>{p['exitR']:+.2f}R</b> ({p['engulfCandles']}C)\n"
            f"📌 Sabab: <b>{p.get('closeReason', 'SL')}</b>\n"
            f"{tp1_info}"
            f"💵 Gross: ${p['gross']:+.2f}\n"
            f"🔻 Komissiya: -${p['commission']:.2f}\n"
            f"💰 <b>Net: ${net_pnl:+.2f}</b>\n"
            f"📈 Balans: <b>${self.balance:.2f}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

        ch = make_chart(self.candles, self.trades, self.symbol,
                        CONFIG['INTERVAL'], "· yopildi",
                        open_positions=self.positions)
        if ch:
            await tg.photo(ch, f"📊 {self.symbol} · yopildi ({p['result']})")

        # Kunlik zarar limiti
        self.checkDayReset()
        day_loss_pct = (self.day_start_balance - self.balance) / self.day_start_balance if self.day_start_balance > 0 else 0
        if day_loss_pct >= CONFIG['MAX_DAILY_LOSS_PCT'] and not self.trading_paused:
            self.trading_paused = True
            await tg.send(
                f"🛑 <b>{self.symbol}: KUNLIK ZARAR LIMITI</b>\n"
                f"📉 Bugungi zarar: -{day_loss_pct*100:.1f}%"
            )


# ============================================================
# GLOBAL ENGINES
# ============================================================
ENGINES = {}


# ============================================================
# PRELOAD
# ============================================================
async def preload_candles(client, eng, symbol):
    try:
        klines = await client.get_klines(
            symbol=symbol,
            interval=CONFIG['INTERVAL'],
            limit=CONFIG['PRELOAD_CANDLES']
        )
        candles = []
        for k in klines[:-1]:
            candles.append({
                'time':   k[0] // 1000,
                'open':   float(k[1]),
                'high':   float(k[2]),
                'low':    float(k[3]),
                'close':  float(k[4]),
                'closed': True,
            })
        eng.candles = candles
        log.info(f"📥 {symbol}: {len(candles)} ta sham yuklandi")
    except Exception as e:
        log.error(f"{symbol} preload xato: {e}")


# ============================================================
# WORKER
# ============================================================
async def worker(client, symbol):
    eng = Engine(symbol)
    ENGINES[symbol] = eng
    log.info(f"🔵 {symbol} worker boshlandi")

    await preload_candles(client, eng, symbol)

    bsm = BinanceSocketManager(client)

    while True:
        try:
            socket = bsm.kline_socket(symbol=symbol, interval=CONFIG['INTERVAL'])
            async with socket as stream:
                log.info(f"🟢 {symbol} socket ulandi")
                while True:
                    msg = await stream.recv()
                    if not msg or msg.get('e') != 'kline':
                        continue

                    eng.last_candle_time = time.time()

                    k = msg['k']
                    candle = {
                        'time':   k['t'] // 1000,
                        'open':   float(k['o']),
                        'high':   float(k['h']),
                        'low':    float(k['l']),
                        'close':  float(k['c']),
                        'closed': k['x'],
                    }

                    if candle['closed']:
                        if eng.candles and eng.candles[-1]['time'] == candle['time']:
                            eng.candles[-1] = candle
                        else:
                            eng.candles.append(candle)
                        if len(eng.candles) > 200: eng.candles.pop(0)

                        idx = len(eng.candles) - 1
                        sig = eng.checkEngulfing(eng.candles, idx)
                        if sig:
                            await eng.openSignal(sig, candle)

                    for p in list(eng.positions):
                        closed = eng.manageLocal(p, candle)
                        if closed:
                            await eng.closeSignal(p)

        except Exception as e:
            log.error(f"{symbol} socket xato: {e} — 5s dan keyin qayta ulanadi")
            await asyncio.sleep(5)


# ============================================================
# HEALTH-CHECK
# ============================================================
async def health_check():
    warned = {}
    while True:
        await asyncio.sleep(60)
        now = time.time()
        timeout_sec = CONFIG['SOCKET_TIMEOUT_MIN'] * 60
        for sym, eng in ENGINES.items():
            if eng.last_candle_time is None: continue
            silent_for = now - eng.last_candle_time
            if silent_for >= timeout_sec and not warned.get(sym, False):
                warned[sym] = True
                await tg.send(f"⚠️ <b>{sym} socket jim!</b> So'nggi {int(silent_for//60)} daqiqadan beri ma'lumot yo'q.")
            elif silent_for < timeout_sec and warned.get(sym, False):
                warned[sym] = False


# ============================================================
# KUNLIK DIAGNOSTIKA
# ============================================================
async def daily_diagnostics():
    sent_today = None
    while True:
        now = datetime.now(timezone.utc)
        today_key = now.strftime('%Y-%m-%d')
        if now.hour == CONFIG['DIAGNOSTICS_HOUR'] and sent_today != today_key:
            sent_today = today_key
            if not ENGINES:
                await asyncio.sleep(60); continue

            ranked = sorted(ENGINES.items(), key=lambda x: (x[1].balance - x[1].initial), reverse=True)
            best_sym, best_eng = ranked[0]
            worst_sym, worst_eng = ranked[-1]

            text = (f"🔍 <b>KUNLIK DIAGNOSTIKA</b>\n"
                    f"📅 {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🏆 Best: {best_sym} ({(best_eng.balance - best_eng.initial):+.2f}$)\n"
                    f"💔 Worst: {worst_sym} ({(worst_eng.balance - worst_eng.initial):+.2f}$)\n\n")

            tot_w = sum(e.stats_2c['wins'] for e in ENGINES.values())
            tot_l = sum(e.stats_2c['losses'] for e in ENGINES.values())
            tot_net = sum(e.stats_2c['net'] for e in ENGINES.values())
            tot_tp1 = sum(e.tp1_hits for e in ENGINES.values())
            tot_tr = sum(e.trail_steps for e in ENGINES.values())
            t = tot_w + tot_l
            wr = tot_w / t * 100 if t > 0 else 0

            text += (f"2C: ✅{tot_w} ❌{tot_l} (WR {wr:.1f}%) | ${tot_net:+.2f}\n"
                     f"TP1 hits: {tot_tp1} | Trail: {tot_tr}\n\n"
                     f"📂 Symbol bo'yicha:\n")
            for sym, e in ENGINES.items():
                net = e.balance - e.initial
                emoji = "🟢" if net >= 0 else "🔴"
                text += f"{emoji} {sym}: {net:+.2f}$\n"

            await tg.send(text)
        await asyncio.sleep(60)


# ============================================================
# KUNLIK HISOBOT
# ============================================================
async def daily_report():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == CONFIG['REPORT_HOUR'] and now.minute < 1:
            total_bal  = sum(e.balance for e in ENGINES.values())
            total_init = sum(e.initial for e in ENGINES.values())
            total_w    = sum(e.wins for e in ENGINES.values())
            total_l    = sum(e.losses for e in ENGINES.values())
            total_be   = sum(e.bes for e in ENGINES.values())
            total_tp1  = sum(e.tp1_hits for e in ENGINES.values())
            total_tr   = sum(e.trail_steps for e in ENGINES.values())
            total_done = total_w + total_l + total_be
            wr  = total_w / total_done * 100 if total_done > 0 else 0
            pct = (total_bal - total_init) / total_init * 100 if total_init else 0

            text = (f"📊 <b>KUNLIK HISOBOT (signal-only)</b>\n"
                    f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n━━━━━━━━━━━━━━━━━━━━━━\n")
            for sym, e in ENGINES.items():
                s  = e.wins + e.losses + e.bes
                sw = e.wins / s * 100 if s > 0 else 0
                sp = (e.balance - e.initial) / e.initial * 100
                emoji = "🟢" if e.balance >= e.initial else "🔴"
                text += (f"\n{emoji} <b>{sym}</b>: ${e.balance:.2f} ({sp:+.2f}%)\n"
                         f"   WR:{sw:.1f}% ✅{e.wins}❌{e.losses}⚪{e.bes}\n"
                         f"   TP1: {e.tp1_hits} | Trail: {e.trail_steps}\n")

            text += (f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                     f"JAMI: WR {wr:.1f}% | Balans ${total_bal:.2f} ({pct:+.2f}%)\n"
                     f"TP1: {total_tp1} | Trail: {total_tr}")
            await tg.send(text)
            await asyncio.sleep(60)
        await asyncio.sleep(30)


# ============================================================
# ASOSIY
# ============================================================
async def main():
    log.info("🚀 Bot v5.7.0 ishga tushdi — SIGNAL-ONLY + BE/TP1/Trail")
    log.info(f"Symbols: {CONFIG['SYMBOLS']} | TF: {CONFIG['INTERVAL']}")
    log.info(f"BE @ 1:{CONFIG['BE_AT_R']} | TP1 {int(CONFIG['TP1_PCT']*100)}% @ 1:{CONFIG['TP1_AT_R']} | Trail {CONFIG['TRAIL_STEP']}R")

    await tg.send(
        f"🚀 <b>Engulfing Bot v5.7.0</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Signal-only — real order YO'Q</b>\n"
        f"📊 Symbols: {', '.join(CONFIG['SYMBOLS'])}\n"
        f"⏱ TF: {CONFIG['INTERVAL']}\n"
        f"⚖️ Risk/trade: {CONFIG['RISK_PCT']*100:.1f}%\n"
        f"🎯 BE @ 1:{CONFIG['BE_AT_R']}\n"
        f"🎯 TP1 {int(CONFIG['TP1_PCT']*100)}% @ 1:{CONFIG['TP1_AT_R']}\n"
        f"🎯 Trail: {CONFIG['TRAIL_STEP']}R qadam\n"
        f"✅ Bot aktiv"
    )

    client = await AsyncClient.create()

    tasks = [asyncio.create_task(worker(client, s)) for s in CONFIG['SYMBOLS']]
    tasks.append(asyncio.create_task(daily_report()))
    tasks.append(asyncio.create_task(health_check()))
    tasks.append(asyncio.create_task(daily_diagnostics()))

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        log.error(f"Main xato: {e}")
    finally:
        await client.close_connection()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("⛔ Bot to'xtatildi")