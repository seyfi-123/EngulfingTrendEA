# ============================================================
# EngulfingTrend Bot v1.0 — Binance Testnet
# 1-shm yoki 2-shm engulfing + BE + TP1 15% + Step Trail
# ============================================================
import os
import asyncio
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
from telegram import Bot

load_dotenv()

# ============================================================
# SOZLAMALAR
# ============================================================
CONFIG = {
    'API_KEY':      os.getenv('BINANCE_API_KEY'),
    'API_SECRET':   os.getenv('BINANCE_API_SECRET'),
    'TESTNET':      os.getenv('TESTNET', 'True') == 'True',
    'SYMBOL':       os.getenv('SYMBOL', 'SOLUSDT'),
    'INTERVAL':     os.getenv('INTERVAL', '5m'),

    # Lot
    'LOT_START':    0.05,
    'LOT_CAP':      0.50,

    # SL
    'SL_BUF_PCT':   0.0001,     # 0.01% SL buffer

    # TP1 va BE
    'TP1_PCT':      0.15,        # 15% qism
    'TP1_AT_R':     2.0,         # 1:2 da yopiladi
    'BE_AT_R':      1.0,         # 1:1 da BE

    # Trail
    'TRAIL_STEP':   0.5,         # 0.5R qadam

    # Komissiya
    'COMM_RATE':    0.0005,      # 0.05%

    # Xavfsizlik
    'MAX_DAILY_LOSS':    5.0,    # 5% kunlik zarar
    'MAX_CONSEC_LOSSES': 5,      # 5 ta ketma-ket
    'COOLDOWN_HOURS':    4,      # 4 soat pauza
}

TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


async def send_telegram(msg):
    """Telegram xabar yuborish"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode='HTML'
        )
    except Exception as e:
        log.error(f"Telegram xato: {e}")


# ============================================================
# TRADE ENGINE
# ============================================================
class TradeEngine:
    def __init__(self):
        self.balance = 1000.0
        self.initial_balance = 1000.0
        self.current_lot = CONFIG['LOT_START']
        self.completed_trades = 0
        self.wins = 0
        self.losses = 0
        self.bes = 0
        self.tp1_hits = 0
        self.trail_steps = 0
        self.pos = None
        self.candles = []
        self.last_trade_time = 0
        self.consec_losses = 0
        self.daily_start_balance = 1000.0
        self.daily_date = datetime.utcnow().date()
        self.paused_until = 0

    def update_lot(self):
        """0.05 → 0.10 → ... → 0.50"""
        bonus = (self.completed_trades // 10) * 0.05
        self.current_lot = min(CONFIG['LOT_START'] + bonus, CONFIG['LOT_CAP'])

    def check_daily_loss(self):
        today = datetime.utcnow().date()
        if today != self.daily_date:
            self.daily_date = today
            self.daily_start_balance = self.balance
            self.consec_losses = 0
            log.info(f"Yangi kun. Balans: ${self.balance:.2f}")
        loss_pct = (self.daily_start_balance - self.balance) / self.daily_start_balance * 100
        if loss_pct >= CONFIG['MAX_DAILY_LOSS']:
            log.warning(f"Kunlik zarar limiti: {loss_pct:.2f}%")
            return True
        return False

    def is_paused(self):
        return time.time() < self.paused_until

    def check_engulfing(self, candles):
        """1 yoki 2 shm engulfing"""
        if len(candles) < 3:
            return None
        cur = candles[-1]
        p1  = candles[-2]
        p2  = candles[-3]

        # 1-shm engulfing
        bull1 = (cur['close'] > cur['open'] and
                 p1['close'] < p1['open'] and
                 cur['open'] <= p1['close'] and
                 cur['close'] >= p1['open'])
        bear1 = (cur['close'] < cur['open'] and
                 p1['close'] > p1['open'] and
                 cur['open'] >= p1['close'] and
                 cur['close'] <= p1['open'])

        # 2-shm engulfing
        max_high2 = max(p1['high'], p2['high'])
        min_low2  = min(p1['low'],  p2['low'])

        bull2 = (cur['close'] > cur['open'] and
                 p1['close'] < p1['open'] and
                 p2['close'] < p2['open'] and
                 cur['low'] <= min_low2 and
                 cur['close'] > max_high2)
        bear2 = (cur['close'] < cur['open'] and
                 p1['close'] > p1['open'] and
                 p2['close'] > p2['open'] and
                 cur['high'] >= max_high2 and
                 cur['close'] < min_low2)

        if bull1 or bull2:
            return {'type': 'BUY',  'candles': 2 if bull2 else 1}
        if bear1 or bear2:
            return {'type': 'SELL', 'candles': 2 if bear2 else 1}
        return None

    async def open_trade(self, client, signal, candle):
        """Yangi pozitsiya ochish"""
        now = time.time()

        if self.is_paused():
            log.info("Pauzada — savdo ochilmaydi")
            return

        # Cooldown: oxirgi savdodan 3 daqiqa
        if now - self.last_trade_time < 180:
            return

        entry = candle['close']
        buf = entry * CONFIG['SL_BUF_PCT']

        if signal['type'] == 'BUY':
            sl = candle['low'] - buf
            sl_dist = entry - sl
        else:
            sl = candle['high'] + buf
            sl_dist = sl - entry

        if sl_dist <= 0:
            return

        # Lot hisoblash: risk 2% balans
        risk_usd = self.balance * 0.02
        risk_per_lot = sl_dist * 100   # 1.0 lot uchun 100x
        qty = risk_usd / risk_per_lot if risk_per_lot > 0 else CONFIG['LOT_START']
        qty = max(round(qty, 3), CONFIG['LOT_START'])
        qty = min(qty, CONFIG['LOT_CAP'])

        try:
            side = SIDE_BUY if signal['type'] == 'BUY' else SIDE_SELL
            order = await client.create_order(
                symbol=CONFIG['SYMBOL'],
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=qty
            )

            fill_price = float(order['fills'][0]['price'])
            log.info(f"✅ {signal['type']} ochildi: {qty} @ ${fill_price}")

            self.pos = {
                'time': now,
                'type': signal['type'],
                'engulf_candles': signal['candles'],
                'entry': fill_price,
                'sl': sl,
                'initial_sl': sl,
                'sl_dist': sl_dist,
                'qty': qty,
                'qty_remaining': qty,
                'be_set': False,
                'tp1_done': False,
                'lock_r': -1,
                'tp1_pnl': 0,
            }
            self.last_trade_time = now

            await send_telegram(
                f"🟢 <b>{signal['type']}</b> {CONFIG['SYMBOL']}\n"
                f"Entry: ${fill_price:.4f}\n"
                f"SL: ${sl:.4f}\n"
                f"Qty: {qty}\n"
                f"Engulf: {signal['candles']}C"
            )
        except Exception as e:
            log.error(f"Order xato: {e}")
            await send_telegram(f"❌ Order xato: {e}")

    async def manage_trade(self, client, candle):
        """Pozitsiyani boshqarish"""
        p = self.pos
        if not p:
            return

        cur_high = candle['high']
        cur_low  = candle['low']

        # SL tegilganini tekshirish
        exit_price = None
        exit_r = 0

        if p['type'] == 'BUY' and cur_low <= p['sl']:
            exit_price = p['sl']
            exit_r = (exit_price - p['entry']) / p['sl_dist']
        elif p['type'] == 'SELL' and cur_high >= p['sl']:
            exit_price = p['sl']
            exit_r = (p['entry'] - exit_price) / p['sl_dist']

        if exit_price is not None:
            # Order yopish
            try:
                side = SIDE_SELL if p['type'] == 'BUY' else SIDE_BUY
                await client.create_order(
                    symbol=CONFIG['SYMBOL'],
                    side=side,
                    type=ORDER_TYPE_MARKET,
                    quantity=p['qty_remaining']
                )
            except Exception as e:
                log.error(f"Close order xato: {e}")
                return

            # P&L
            gross = exit_r * p['qty_remaining'] * p['sl_dist'] * 100
            comm  = p['qty_remaining'] * exit_price * CONFIG['COMM_RATE']
            net   = gross - comm

            self.balance += net
            self.completed_trades += 1
            self.update_lot()

            if net > 0.5:
                self.wins += 1
                self.consec_losses = 0
            elif net < -0.5:
                self.losses += 1
                self.consec_losses += 1
                if self.consec_losses >= CONFIG['MAX_CONSEC_LOSSES']:
                    self.paused_until = time.time() + CONFIG['COOLDOWN_HOURS'] * 3600
                    await send_telegram(
                        f"⚠️ {CONFIG['MAX_CONSEC_LOSSES']} zarar ketma-ket. "
                        f"{CONFIG['COOLDOWN_HOURS']}s pauza."
                    )
            else:
                self.bes += 1

            log.info(f"Yopildi: {exit_r:+.2f}R, net ${net:+.2f}")

            await send_telegram(
                f"🔴 <b>Yopildi</b> {p['type']}\n"
                f"Exit: ${exit_price:.4f}\n"
                f"Exit R: {exit_r:+.2f}R\n"
                f"Net: ${net:+.2f}\n"
                f"Balans: ${self.balance:.2f}"
            )
            self.pos = None
            return

        # Max R hisoblash
        if p['type'] == 'BUY':
            max_r = (cur_high - p['entry']) / p['sl_dist']
        else:
            max_r = (p['entry'] - cur_low) / p['sl_dist']

        # 1:1 → BE
        if max_r >= CONFIG['BE_AT_R'] and not p['be_set']:
            p['sl'] = p['entry']
            p['be_set'] = True
            log.info(f"BE o'rnatildi: {p['type']}")

        # 1:2 → TP1 15%
        if max_r >= CONFIG['TP1_AT_R'] and not p['tp1_done']:
            p['tp1_done'] = True
            tp1_qty = p['qty'] * CONFIG['TP1_PCT']

            try:
                side = SIDE_SELL if p['type'] == 'BUY' else SIDE_BUY
                await client.create_order(
                    symbol=CONFIG['SYMBOL'],
                    side=side,
                    type=ORDER_TYPE_MARKET,
                    quantity=tp1_qty
                )
                p['qty_remaining'] -= tp1_qty
                self.tp1_hits += 1
                log.info(f"TP1 15% yopildi: {tp1_qty}")
                await send_telegram(
                    f"💰 TP1 @ 1:2\n"
                    f"Yopildi: {tp1_qty}\n"
                    f"Qoldi: {p['qty_remaining']}"
                )
            except Exception as e:
                log.error(f"TP1 xato: {e}")

        # Step Trail (0.5R qadam)
        if max_r >= CONFIG['BE_AT_R']:
            steps = int(max_r / CONFIG['TRAIL_STEP'])
            lock_r = (steps - 2) * CONFIG['TRAIL_STEP']

            if lock_r >= 0:
                if p['type'] == 'BUY':
                    new_sl = p['entry'] + lock_r * p['sl_dist']
                    if new_sl > p['sl']:
                        p['sl'] = new_sl
                        p['lock_r'] = lock_r
                        self.trail_steps += 1
                else:
                    new_sl = p['entry'] - lock_r * p['sl_dist']
                    if new_sl < p['sl']:
                        p['sl'] = new_sl
                        p['lock_r'] = lock_r
                        self.trail_steps += 1


# ============================================================
# ASOSIY
# ============================================================
async def main():
    log.info("🚀 Bot ishga tushdi")
    log.info(f"Symvol: {CONFIG['SYMBOL']} | TF: {CONFIG['INTERVAL']}")
    log.info(f"TESTNET: {CONFIG['TESTNET']}")

    await send_telegram(
        f"🚀 <b>Engulfing Bot</b> ishga tushdi\n"
        f"Symvol: {CONFIG['SYMBOL']}\n"
        f"TF: {CONFIG['INTERVAL']}\n"
        f"TESTNET: {CONFIG['TESTNET']}"
    )

    client = await AsyncClient.create(
        api_key=CONFIG['API_KEY'],
        api_secret=CONFIG['API_SECRET'],
        testnet=CONFIG['TESTNET']
    )

    bsm = BinanceSocketManager(client)
    engine = TradeEngine()

    socket = bsm.kline_socket(CONFIG['SYMBOL'], interval=CONFIG['INTERVAL'])

    async with socket as stream:
        async for msg in stream:
            if msg.get('e') != 'kline':
                continue
            k = msg['k']

            candle = {
                'time':   k['t'] // 1000,
                'open':   float(k['o']),
                'high':   float(k['h']),
                'low':    float(k['l']),
                'close':  float(k['c']),
                'closed': k['x'],
            }

            # Yangi shm yopilganda signal tekshir
            if candle['closed']:
                engine.candles.append(candle)
                if len(engine.candles) > 100:
                    engine.candles.pop(0)

                # Kunlik zarar
                if engine.check_daily_loss():
                    await send_telegram("🛑 Kunlik zarar limiti!")
                    continue

                # Signal
                if not engine.pos:
                    signal = engine.check_engulfing(engine.candles)
                    if signal:
                        await engine.open_trade(client, signal, candle)

            # Aktiv pozitsiyani boshqar
            if engine.pos:
                await engine.manage_trade(client, candle)

    await client.close_connection()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot to'xtatildi")
