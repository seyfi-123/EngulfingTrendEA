# ============================================================
# EngulfingTrend Bot v5.5 — FINAL (Diagnostics + Health-check + Daily Loss Guard)
# ============================================================
import os
import asyncio
import logging
import time
import io
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
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
    'API_KEY':    os.getenv('BINANCE_API_KEY'),
    'API_SECRET': os.getenv('BINANCE_API_SECRET'),
    'TESTNET':    os.getenv('TESTNET', 'True') == 'True',
    'SYMBOLS':    [s.strip().upper() for s in os.getenv('SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT').split(',') if s.strip()],
    'INTERVAL':   os.getenv('INTERVAL', '5m'),
    'BALANCE':    float(os.getenv('BALANCE', '1000')),
    'RISK_PCT':   float(os.getenv('RISK_PCT', '0.05')),
    'LOT_MIN':    float(os.getenv('LOT_MIN', '0.001')),
    'LOT_MAX':    float(os.getenv('LOT_MAX', '2.0')),
    'MAX_OPEN_POS': int(os.getenv('MAX_OPEN_POS', '5')),
    'PRELOAD_CANDLES': int(os.getenv('PRELOAD_CANDLES', '100')),
    'SL_BUF':     10,
    'BE_AT_R':    1.0,
    'TRAIL_STEP': 1.0,
    'COMM_RATE':  0.0005,
    'CHART_CANDLES': 100,
    'REPORT_HOUR':   18,

    # --- YANGI: kuzatuv va himoya sozlamalari (strategiyaga tegmaydi) ---
    'SOCKET_TIMEOUT_MIN':  int(os.getenv('SOCKET_TIMEOUT_MIN', '30')),   # necha daqiqa candle kelmasa ogohlantirish
    'MAX_DAILY_LOSS_PCT':  float(os.getenv('MAX_DAILY_LOSS_PCT', '0.15')),  # kunlik max zarar % (balансdan)
    'DIAGNOSTICS_HOUR':    int(os.getenv('DIAGNOSTICS_HOUR', '9')),      # kunlik diagnostika xabari soati (UTC)
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
# GRAFIK YARATISH
# ============================================================
def make_chart(candles, trades, symbol, interval, suffix=""):
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

        for t in trades[-30:]:
            try:
                tt = datetime.fromtimestamp(t['time'])
                if tt in df.index:
                    i = list(df.index).index(tt)
                    c = '#10b981' if t['type'] == 'B' else '#ef4444'
                    m = '^' if t['type'] == 'B' else 'v'
                    ax.scatter(i, t['entry'], color=c, marker=m, s=120, zorder=5)
            except: pass

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
# TRADE ENGINE
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
        self.positions = []
        self.candles = []
        self.trades = []
        self.total_comm = 0.0
        self.gross_pnl = 0.0

        # --- YANGI: diagnostika uchun statistika (faqat kuzatuv, mantiqqa ta'sir qilmaydi) ---
        self.last_candle_time = None          # oxirgi kline qachon kelgani (health-check uchun)
        self.stats_1c = {'wins': 0, 'losses': 0, 'net': 0.0}   # 1-candle engulfing statistikasi
        self.stats_2c = {'wins': 0, 'losses': 0, 'net': 0.0}   # 2-candle engulfing statistikasi
        self.day_start_balance = CONFIG['BALANCE']  # kun boshidagi balans (daily loss guard uchun)
        self.day_key = None                    # joriy kun (YYYY-MM-DD), kun almashganda reset qilish uchun
        self.trading_paused = False            # daily loss limitiga yetsa True bo'ladi

    def calcLot(self, slDist, price):
        risk_amount = self.balance * CONFIG['RISK_PCT']
        if slDist <= 0 or price <= 0:
            return CONFIG['LOT_MIN']

        lot = risk_amount / slDist
        lot = max(CONFIG['LOT_MIN'], min(lot, CONFIG['LOT_MAX']))

        max_lot_by_balance = self.balance / price
        lot = min(lot, max_lot_by_balance)

        return round(lot, 3)

    def checkEngulfing(self, cd, idx):
        if idx < 2: return None
        cur = cd[idx]; p1 = cd[idx - 1]; p2 = cd[idx - 2]

        bull1 = (cur['close'] > cur['open'] and cur['open'] <= p1['close']
                 and cur['close'] >= p1['open'] and p1['close'] < p1['open'])
        bear1 = (cur['close'] < cur['open'] and cur['open'] >= p1['close']
                 and cur['close'] <= p1['open'] and p1['close'] > p1['open'])

        maxHigh2 = max(p1['high'], p2['high'])
        minLow2  = min(p1['low'],  p2['low'])
        bull2 = (cur['close'] > cur['open'] and p1['close'] < p1['open']
                 and p2['close'] < p2['open'] and cur['low'] <= minLow2
                 and cur['close'] > maxHigh2)
        bear2 = (cur['close'] < cur['open'] and p1['close'] > p1['open']
                 and p2['close'] > p2['open'] and cur['high'] >= maxHigh2
                 and cur['close'] < minLow2)

        if bull1 or bull2: return {'type': 'B', 'candles': 2 if bull2 else 1}
        if bear1 or bear2: return {'type': 'S', 'candles': 2 if bear2 else 1}
        return None

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
            'riskPerR': lot * slDist,
            'balance_at_entry': self.balance,
        }

    def manageLocal(self, p, candle):
        exit_price = None
        exitR = 0

        if p['type'] == 'B' and candle['low'] <= p['sl']:
            exit_price = p['sl']
            exitR = (exit_price - p['entry']) / p['slDist']
        elif p['type'] == 'S' and candle['high'] >= p['sl']:
            exit_price = p['sl']
            exitR = (p['entry'] - exit_price) / p['slDist']

        if exit_price is not None:
            p['pnl'] = exitR * p['riskPerR']
            p['exit'] = exit_price
            p['exitR'] = exitR
            return True

        if p['type'] == 'B':
            maxR = (candle['high'] - p['entry']) / p['slDist']
        else:
            maxR = (p['entry'] - candle['low']) / p['slDist']

        if maxR >= CONFIG['BE_AT_R']:
            trailR = int(maxR) - 1
            if p['type'] == 'B':
                newSL = p['entry'] + trailR * p['slDist']
                if newSL > p['sl']: p['sl'] = newSL
            else:
                newSL = p['entry'] - trailR * p['slDist']
                if newSL < p['sl']: p['sl'] = newSL

        return False

    # --- YANGI: kun almashganda kunlik balans hisobini reset qilish ---
    def checkDayReset(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.day_key != today:
            self.day_key = today
            self.day_start_balance = self.balance
            if self.trading_paused:
                log.info(f"{self.symbol}: yangi kun boshlandi, savdo qayta yoqildi")
            self.trading_paused = False

    async def openReal(self, client, signal, candle):
        self.checkDayReset()

        # --- YANGI: kunlik zarar limiti tekshiruvi (faqat himoya, entry logikasiga tegmaydi) ---
        if self.trading_paused:
            log.info(f"{self.symbol}: kunlik zarar limiti tufayli savdo to'xtatilgan, signal o'tkazib yuborildi")
            return

        if len(self.positions) >= CONFIG['MAX_OPEN_POS']:
            log.info(f"{self.symbol}: max pozitsiya limiti ({CONFIG['MAX_OPEN_POS']}) yetdi, signal o'tkazib yuborildi")
            return

        p = self.openLocal(signal, candle)
        if not p: return

        try:
            side = SIDE_BUY if signal['type'] == 'B' else SIDE_SELL
            order = await client.create_order(
                symbol=self.symbol, side=side,
                type=ORDER_TYPE_MARKET, quantity=p['lot']
            )
            fill = float(order['fills'][0]['price'])
            p['entry'] = fill

            if p['type'] == 'B':
                p['sl'] = fill - p['slDist']
            else:
                p['sl'] = fill + p['slDist']

            self.positions.append(p)
            log.info(f"✅ {self.symbol} {signal['type']} @ ${fill} | lot={p['lot']} | risk=${p['riskPerR']:.2f} | ochiq: {len(self.positions)}")

            action = "BUY" if signal['type'] == 'B' else "SELL"
            emoji = "🟢" if signal['type'] == 'B' else "🔴"
            await tg.send(
                f"{emoji} <b>{action} {self.symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Entry: <b>${fill:.4f}</b>\n"
                f"🛡 SL: ${p['sl']:.4f}\n"
                f"💰 Lot: {p['lot']} (risk: ${p['riskPerR']:.2f})\n"
                f"🎯 Engulf: <b>{p['engulfCandles']}C</b>\n"
                f"📂 Ochiq pozitsiyalar: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}\n"
                f"💵 Balans: ${self.balance:.2f}\n"
                f"⏰ {datetime.now().strftime('%H:%M:%S')}"
            )

            ch = make_chart(self.candles, self.trades, self.symbol,
                            CONFIG['INTERVAL'], f"· {action}")
            if ch:
                await tg.photo(ch, f"📊 {self.symbol} · {action}")

        except Exception as e:
            log.error(f"{self.symbol} order xato: {e}")

    async def closeReal(self, client, p):
        try:
            side = SIDE_SELL if p['type'] == 'B' else SIDE_BUY
            await client.create_order(
                symbol=self.symbol, side=side,
                type=ORDER_TYPE_MARKET, quantity=p['lot']
            )
        except Exception as e:
            log.error(f"{self.symbol} close xato: {e}")
            return

        open_comm  = p['lot'] * p['entry'] * CONFIG['COMM_RATE']
        close_comm = p['lot'] * p['exit']  * CONFIG['COMM_RATE']
        total_comm = open_comm + close_comm
        net_pnl = p['pnl'] - total_comm

        self.balance    += net_pnl
        self.gross_pnl  += p['pnl']
        self.total_comm += total_comm

        p['commission'] = total_comm
        p['net_pnl']    = net_pnl
        self.completedTrades += 1

        if net_pnl > 0.01:
            p['result'] = 'W'; self.wins += 1
        elif net_pnl < -0.01:
            p['result'] = 'L'; self.losses += 1
        else:
            p['result'] = 'BE'; self.bes += 1

        # --- YANGI: engulfing turi bo'yicha statistika (faqat kuzatuv) ---
        target_stats = self.stats_1c if p['engulfCandles'] == 1 else self.stats_2c
        target_stats['net'] += net_pnl
        if net_pnl > 0.01:
            target_stats['wins'] += 1
        elif net_pnl < -0.01:
            target_stats['losses'] += 1

        self.trades.append(p)
        self.positions.remove(p)

        log.info(f"{self.symbol}: {p['exitR']:+.2f}R | gross ${p['pnl']:+.2f} | comm -${total_comm:.2f} | net ${net_pnl:+.2f} | balans ${self.balance:.2f} | ochiq: {len(self.positions)}")

        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        await tg.send(
            f"{emoji} <b>Yopildi {self.symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Exit: ${p['exit']:.4f}\n"
            f"🎯 R: <b>{p['exitR']:+.2f}R</b> ({p['engulfCandles']}C)\n"
            f"💵 Gross: ${p['pnl']:+.2f}\n"
            f"🔻 Komissiya: -${total_comm:.2f}\n"
            f"💰 <b>Net: ${net_pnl:+.2f}</b>\n"
            f"📂 Ochiq pozitsiyalar: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}\n"
            f"📈 Balans: <b>${self.balance:.2f}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

        # --- YANGI: kunlik zarar limiti tekshiruvi ---
        self.checkDayReset()
        day_loss_pct = (self.day_start_balance - self.balance) / self.day_start_balance if self.day_start_balance > 0 else 0
        if day_loss_pct >= CONFIG['MAX_DAILY_LOSS_PCT'] and not self.trading_paused:
            self.trading_paused = True
            await tg.send(
                f"🛑 <b>{self.symbol}: KUNLIK ZARAR LIMITI!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📉 Bugungi zarar: <b>-{day_loss_pct*100:.1f}%</b>\n"
                f"💵 Kun boshi balans: ${self.day_start_balance:.2f}\n"
                f"💵 Hozirgi balans: ${self.balance:.2f}\n"
                f"⏸ Bu symbol uchun yangi savdo ertangi kungacha to'xtatildi.\n"
                f"(Ochiq pozitsiyalar hali ham SL/trailing bilan boshqariladi)"
            )


# ============================================================
# GLOBAL ENGINES
# ============================================================
ENGINES = {}


# ============================================================
# TARIXIY CANDLE'LARNI OLDINDAN YUKLASH
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
        log.info(f"📥 {symbol}: {len(candles)} ta tarixiy candle yuklandi")
    except Exception as e:
        log.error(f"{symbol} preload xato: {e}")


# ============================================================
# WORKER (BinanceSocketManager bilan — async, barqaror)
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

                    # --- YANGI: har kelgan kline vaqtini yozib boramiz (health-check uchun) ---
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
                            await eng.openReal(client, sig, candle)

                    for p in list(eng.positions):
                        closed = eng.manageLocal(p, candle)
                        if closed:
                            await eng.closeReal(client, p)

        except Exception as e:
            log.error(f"{symbol} socket xato: {e} — 5s dan keyin qayta ulanadi")
            await asyncio.sleep(5)


# ============================================================
# YANGI: HEALTH-CHECK — socket 30+ daqiqa "jim" qolsa ogohlantirish
# ============================================================
async def health_check():
    warned = {}  # symbol -> allaqachon ogohlantirilganmi (spam bo'lmasligi uchun)
    while True:
        await asyncio.sleep(60)  # har daqiqa tekshiradi
        now = time.time()
        timeout_sec = CONFIG['SOCKET_TIMEOUT_MIN'] * 60

        for sym, eng in ENGINES.items():
            if eng.last_candle_time is None:
                continue
            silent_for = now - eng.last_candle_time

            if silent_for >= timeout_sec and not warned.get(sym, False):
                warned[sym] = True
                minutes = int(silent_for // 60)
                await tg.send(
                    f"⚠️ <b>DIQQAT: {sym} socket jim!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"So'nggi {minutes} daqiqadan beri yangi ma'lumot kelmayapti.\n"
                    f"Websocket uzilib qolgan yoki qayta ulanish jarayonida bo'lishi mumkin.\n"
                    f"Deploy Logs'ni tekshirib ko'ring."
                )
            elif silent_for < timeout_sec and warned.get(sym, False):
                warned[sym] = False  # yana ma'lumot kela boshlasa, keyingi safar yana ogohlantirish uchun reset


# ============================================================
# YANGI: KUNLIK DIAGNOSTIKA — "qayerda ko'p xato, qayerda yaxshi"
# ============================================================
async def daily_diagnostics():
    sent_today = None
    while True:
        now = datetime.now(timezone.utc)
        today_key = now.strftime('%Y-%m-%d')

        if now.hour == CONFIG['DIAGNOSTICS_HOUR'] and sent_today != today_key:
            sent_today = today_key

            if not ENGINES:
                await asyncio.sleep(60)
                continue

            # Har symbol bo'yicha net PnL bo'yicha saralash
            ranked = sorted(
                ENGINES.items(),
                key=lambda x: (x[1].balance - x[1].initial),
                reverse=True
            )

            best_sym, best_eng = ranked[0]
            worst_sym, worst_eng = ranked[-1]

            text = (f"🔍 <b>KUNLIK DIAGNOSTIKA</b>\n"
                    f"📅 {now.strftime('%d.%m.%Y')}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n")

            text += f"🏆 <b>Eng yaxshi:</b> {best_sym} ({(best_eng.balance - best_eng.initial):+.2f}$)\n"
            text += f"💔 <b>Eng yomon:</b> {worst_sym} ({(worst_eng.balance - worst_eng.initial):+.2f}$)\n\n"

            text += "<b>📊 Engulfing turi bo'yicha (barcha symbollar jamlanган):</b>\n"
            total_1c_w = sum(e.stats_1c['wins'] for e in ENGINES.values())
            total_1c_l = sum(e.stats_1c['losses'] for e in ENGINES.values())
            total_1c_net = sum(e.stats_1c['net'] for e in ENGINES.values())
            total_2c_w = sum(e.stats_2c['wins'] for e in ENGINES.values())
            total_2c_l = sum(e.stats_2c['losses'] for e in ENGINES.values())
            total_2c_net = sum(e.stats_2c['net'] for e in ENGINES.values())

            t1 = total_1c_w + total_1c_l
            t2 = total_2c_w + total_2c_l
            wr1 = total_1c_w / t1 * 100 if t1 > 0 else 0
            wr2 = total_2c_w / t2 * 100 if t2 > 0 else 0

            text += (f"├ 1-Candle: ✅{total_1c_w} ❌{total_1c_l} (WR {wr1:.1f}%) | Net: ${total_1c_net:+.2f}\n"
                     f"└ 2-Candle: ✅{total_2c_w} ❌{total_2c_l} (WR {wr2:.1f}%) | Net: ${total_2c_net:+.2f}\n\n")

            if total_1c_net < 0 and total_2c_net >= 0:
                text += "💡 <i>1-Candle engulfing ko'proq zarar keltiryapti, 2-Candle nisbatan yaxshiroq ishlayapti.</i>\n"
            elif total_2c_net < 0 and total_1c_net >= 0:
                text += "💡 <i>2-Candle engulfing ko'proq zarar keltiryapti, 1-Candle nisbatan yaxshiroq ishlayapti.</i>\n"
            elif total_1c_net < 0 and total_2c_net < 0:
                text += "⚠️ <i>Ikkala engulfing turi ham zarar qilmoqda — strategiya parametrlarini qayta ko'rib chiqish tavsiya etiladi.</i>\n"

            text += "\n<b>📂 Symbol bo'yicha holat:</b>\n"
            for sym, e in ENGINES.items():
                net = e.balance - e.initial
                emoji = "🟢" if net >= 0 else "🔴"
                pause_note = " ⏸(to'xtatilgan)" if e.trading_paused else ""
                text += f"{emoji} {sym}: {net:+.2f}${pause_note}\n"

            await tg.send(text)

        await asyncio.sleep(60)


# ============================================================
# KUNLIK HISOBOT
# ============================================================
async def daily_report():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == CONFIG['REPORT_HOUR'] and now.minute < 1:
            log.info("📊 Kunlik hisobot yuborilmoqda...")
            total_bal  = sum(e.balance for e in ENGINES.values())
            total_init = sum(e.initial for e in ENGINES.values())
            total_w    = sum(e.wins for e in ENGINES.values())
            total_l    = sum(e.losses for e in ENGINES.values())
            total_be   = sum(e.bes for e in ENGINES.values())
            total_comm = sum(e.total_comm for e in ENGINES.values())
            total_done = total_w + total_l + total_be
            wr  = total_w / total_done * 100 if total_done > 0 else 0
            pct = (total_bal - total_init) / total_init * 100 if total_init else 0

            text = (f"📊 <b>KUNLIK HISOBOT</b>\n"
                    f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n<b>📈 Symbols:</b>\n")
            for sym, e in ENGINES.items():
                s  = e.wins + e.losses + e.bes
                sw = e.wins / s * 100 if s > 0 else 0
                sp = (e.balance - e.initial) / e.initial * 100
                emoji = "🟢" if e.balance >= e.initial else "🔴"
                text += (f"\n{emoji} <b>{sym}</b>\n"
                         f"├ Balans: ${e.balance:.2f}\n"
                         f"├ O'sish: {sp:+.2f}%\n"
                         f"├ WR: {sw:.1f}%\n"
                         f"├ ✅{e.wins} ❌{e.losses} ⚪{e.bes}\n"
                         f"├ 📂 Ochiq: {len(e.positions)}\n"
                         f"└ 🔻 Komissiya: -${e.total_comm:.2f}\n")

            text += (f"\n━━━━━━━━━━━━━━━━━━━━━━\n<b>📊 JAMI:</b>\n"
                     f"├ ✅ W: {total_w} | ❌ L: {total_l} | ⚪ BE: {total_be}\n"
                     f"├ 🎯 Win Rate: <b>{wr:.1f}%</b>\n"
                     f"├ 💵 Umumiy balans: <b>${total_bal:.2f}</b>\n"
                     f"├ 🔻 Umumiy komissiya: <b>-${total_comm:.2f}</b>\n"
                     f"└ 📈 Umumiy o'sish: <b>{pct:+.2f}%</b>")
            await tg.send(text)

            for sym, e in ENGINES.items():
                if e.candles:
                    ch = make_chart(e.candles, e.trades, sym, CONFIG['INTERVAL'], "· kunlik")
                    if ch:
                        s  = e.wins + e.losses + e.bes
                        sw = e.wins / s * 100 if s > 0 else 0
                        await tg.photo(ch, f"📊 <b>{sym}</b> · WR: {sw:.1f}% · Balans: ${e.balance:.2f}")
            await asyncio.sleep(60)
        await asyncio.sleep(30)


# ============================================================
# ASOSIY
# ============================================================
async def main():
    log.info("🚀 Bot v5.5 ishga tushdi")
    log.info(f"Symbols: {CONFIG['SYMBOLS']}")
    log.info(f"TF: {CONFIG['INTERVAL']} | TESTNET: {CONFIG['TESTNET']}")
    log.info(f"Risk/trade: {CONFIG['RISK_PCT']*100:.1f}% | Max pozitsiya: {CONFIG['MAX_OPEN_POS']} | Komissiya: {CONFIG['COMM_RATE']*100:.2f}%")
    log.info(f"Preload candles: {CONFIG['PRELOAD_CANDLES']}")
    log.info(f"Socket timeout: {CONFIG['SOCKET_TIMEOUT_MIN']}min | Max daily loss: {CONFIG['MAX_DAILY_LOSS_PCT']*100:.0f}%")

    await tg.send(
        f"🚀 <b>Engulfing Bot v5.5</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Symbols: <b>{', '.join(CONFIG['SYMBOLS'])}</b>\n"
        f"⏱ TF: {CONFIG['INTERVAL']}\n"
        f"🔒 TESTNET: {CONFIG['TESTNET']}\n"
        f"⚖️ Risk/trade: {CONFIG['RISK_PCT']*100:.1f}%\n"
        f"📂 Max pozitsiya (symbolga): {CONFIG['MAX_OPEN_POS']}\n"
        f"💰 Komissiya: {CONFIG['COMM_RATE']*100:.2f}%\n"
        f"📥 Preload: {CONFIG['PRELOAD_CANDLES']} candle\n"
        f"🩺 Socket timeout ogohlantirish: {CONFIG['SOCKET_TIMEOUT_MIN']} min\n"
        f"🛑 Max kunlik zarar: {CONFIG['MAX_DAILY_LOSS_PCT']*100:.0f}%\n"
        f"💵 Balans: $1000 × {len(CONFIG['SYMBOLS'])}\n"
        f"✅ Bot aktiv"
    )

    client = await AsyncClient.create(
        api_key=CONFIG['API_KEY'],
        api_secret=CONFIG['API_SECRET'],
        testnet=CONFIG['TESTNET']
    )

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
