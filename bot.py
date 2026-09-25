# ============================================================
# EngulfingTrend Bot v5.0 — FINAL (Tuzatilgan)
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
    'SYMBOLS':    os.getenv('SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT').split(','),
    'INTERVAL':   os.getenv('INTERVAL', '5m'),
    'BALANCE':    1000.0,
    'LOT_START':  0.05,
    'LOT_CAP':    0.50,
    'SL_BUF':     10,
    'BE_AT_R':    1.0,
    'TRAIL_STEP': 1.0,
    'COMM_RATE':  0.0005,
    'CHART_CANDLES': 100,
    'REPORT_HOUR':   18,
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
        self.currentLot = CONFIG['LOT_START']
        self.completedTrades = 0
        self.wins = 0
        self.losses = 0
        self.bes = 0
        self.pos = None
        self.candles = []
        self.trades = []
        self.total_comm = 0.0
        self.gross_pnl = 0.0

    def updateLot(self):
        bonus = (self.completedTrades // 10) * 0.05
        self.currentLot = min(CONFIG['LOT_START'] + bonus, CONFIG['LOT_CAP'])

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

        return {
            'time': cur['time'],
            'type': signal['type'],
            'engulfCandles': signal['candles'],
            'entry': cur['close'],
            'sl': slPrice,
            'initialSL': slPrice,
            'slDist': slDist,
            'lot': self.currentLot,
            'riskPerR': self.currentLot * slDist,
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

    async def openReal(self, client, signal, candle):
        p = self.openLocal(signal, candle)
        if not p: return

        try:
            # ✅ side ni to'g'ri aniqlash
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

            self.pos = p
            log.info(f"✅ {self.symbol} {signal['type']} @ ${fill}")

            # ✅ Xabar uchun 'B' -> 'BUY', 'S' -> 'SELL'
            action = "BUY" if signal['type'] == 'B' else "SELL"
            emoji = "🟢" if signal['type'] == 'B' else "🔴"
            await tg.send(
                f"{emoji} <b>{action} {self.symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Entry: <b>${fill:.4f}</b>\n"
                f"🛡 SL: ${p['sl']:.4f}\n"
                f"💰 Lot: {p['lot']}\n"
                f"🎯 Engulf: <b>{p['engulfCandles']}C</b>\n"
                f"💵 Balans: ${self.balance:.2f}\n"
                f"⏰ {datetime.now().strftime('%H:%M:%S')}"
            )

            ch = make_chart(self.candles, self.trades, self.symbol,
                            CONFIG['INTERVAL'], f"· {action}")
            if ch:
                await tg.photo(ch, f"📊 {self.symbol} · {action}")

        except Exception as e:
            log.error(f"{self.symbol} order xato: {e}")

    async def closeReal(self, client):
        p = self.pos
        if not p: return

        try:
            # ✅ side ni to'g'ri aniqlash
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
        self.updateLot()

        if net_pnl > 0.01:
            p['result'] = 'W'; self.wins += 1
        elif net_pnl < -0.01:
            p['result'] = 'L'; self.losses += 1
        else:
            p['result'] = 'BE'; self.bes += 1

        self.trades.append(p)

        log.info(f"{self.symbol}: {p['exitR']:+.2f}R | gross ${p['pnl']:+.2f} | comm -${total_comm:.2f} | net ${net_pnl:+.2f}")

        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        await tg.send(
            f"{emoji} <b>Yopildi {self.symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Exit: ${p['exit']:.4f}\n"
            f"🎯 R: <b>{p['exitR']:+.2f}R</b>\n"
            f"💵 Gross: ${p['pnl']:+.2f}\n"
            f"🔻 Komissiya: -${total_comm:.2f}\n"
            f"💰 <b>Net: ${net_pnl:+.2f}</b>\n"
            f"📈 Balans: <b>${self.balance:.2f}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        self.pos = None


# ============================================================
# GLOBAL ENGINES
# ============================================================
ENGINES = {}


# ============================================================
# WORKER
# ============================================================
async def worker(client, symbol):
    eng = Engine(symbol)
    ENGINES[symbol] = eng
    log.info(f"🔵 {symbol} worker boshlandi")

    while True:
        try:
            # ✅ Har safar yangi socket yaratish (qayta ulanish uchun)
            bsm = BinanceSocketManager(client)
            sock = bsm.kline_socket(symbol, interval=CONFIG['INTERVAL'])
            async with sock as stream:
                async for msg in stream:
                    if msg.get('e') != 'kline': continue
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
                        eng.candles.append(candle)
                        if len(eng.candles) > 200: eng.candles.pop(0)
                        if not eng.pos:
                            idx = len(eng.candles) - 1
                            sig = eng.checkEngulfing(eng.candles, idx)
                            if sig:
                                await eng.openReal(client, sig, candle)
                    if eng.pos:
                        closed = eng.manageLocal(eng.pos, candle)
                        if closed:
                            await eng.closeReal(client)
        except Exception as e:
            log.error(f"{symbol} WS xato: {e} — 5s kutish")
            await asyncio.sleep(5)


# ============================================================
# KUNLIK HISOBOT
# ============================================================
async def daily_report():
    while True:
        # ✅ UTC vaqtni to'g'ri olish
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
    log.info("🚀 Bot v5.0 ishga tushdi")
    log.info(f"Symbols: {CONFIG['SYMBOLS']}")
    log.info(f"TF: {CONFIG['INTERVAL']} | TESTNET: {CONFIG['TESTNET']}")
    log.info(f"Komissiya: {CONFIG['COMM_RATE']*100:.2f}%")

    await tg.send(
        f"🚀 <b>Engulfing Bot v5.0</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Symbols: <b>{', '.join(CONFIG['SYMBOLS'])}</b>\n"
        f"⏱ TF: {CONFIG['INTERVAL']}\n"
        f"🔒 TESTNET: {CONFIG['TESTNET']}\n"
        f"💰 Komissiya: {CONFIG['COMM_RATE']*100:.2f}%\n"
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

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        log.error(f"Main xato: {e}")
        await client.close_connection()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("⛔ Bot to'xtatildi")
