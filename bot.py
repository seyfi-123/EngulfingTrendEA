# ============================================================
# EngulfingTrend Bot v5.0 — Fast Entry (Mid-Candle)
# ETH/BNB/SOL × 15m
# 2-candle engulfing · Multiple positions · Closed-candle SL/BE/Trail
# ============================================================
import os
import asyncio
import logging
import time
from io import BytesIO
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
from telegram import Bot

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

load_dotenv()

# ============================================================
# SOZLAMALAR
# ============================================================
CONFIG = {
    'API_KEY':      os.getenv('BINANCE_API_KEY'),
    'API_SECRET':   os.getenv('BINANCE_API_SECRET'),
    'TESTNET':      os.getenv('TESTNET', 'True') == 'True',

    'SYMBOLS':      ['ETHUSDT', 'BNBUSDT', 'SOLUSDT'],
    'TIMEFRAME':    '15m',
    'MAX_POSITIONS': 5,          # То 5 позиция ҳамзамон

    'SL_BUF':       10,
    'LOT_START':    0.05,
    'LOT_CAP':      0.50,
    'COMM_RATE':    0.0005,

    'TP1_PCT':      0.15,
    'TP1_AT_R':     2.0,
    'BE_AT_R':      1.0,
    'TRAIL_STEP':   0.5,

    'MAX_DAILY_LOSS':    5.0,
    'MAX_CONSEC_LOSSES': 5,
    'COOLDOWN_HOURS':    4,
    'ENTRY_COOLDOWN':    30,     # 30 сония байни кушоданҳо
}

TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ============================================================
# TELEGRAM
# ============================================================
async def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode='HTML')
    except Exception as e:
        log.error(f"Telegram xato: {e}")


async def send_photo(img_bytes, caption):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        await bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=img_bytes,
                             caption=caption, parse_mode='HTML')
    except Exception as e:
        log.error(f"Telegram photo xato: {e}")


# ============================================================
# ГРАФИК
# ============================================================
def create_chart(symbol, candles, signal, positions):
    fig, ax = plt.subplots(figsize=(10, 5), dpi=100)
    fig.patch.set_facecolor('#0a0b0f')
    ax.set_facecolor('#0a0b0f')

    show = candles[-50:]
    for i, c in enumerate(show):
        color = '#10b981' if c['close'] >= c['open'] else '#ef4444'
        ax.plot([i, i], [c['low'], c['high']], color=color, linewidth=0.8)
        body_low = min(c['open'], c['close'])
        body_high = max(c['open'], c['close'])
        if body_high > body_low:
            rect = Rectangle((i - 0.35, body_low), 0.7,
                             body_high - body_low,
                             facecolor=color, edgecolor=color)
            ax.add_patch(rect)

    n = len(show)
    if signal and n > 0:
        sig_c = show[-1]
        if signal['type'] == 'B':
            ax.annotate('BUY', xy=(n-1, sig_c['low']),
                        color='#6ee7b7', fontsize=12, fontweight='bold',
                        ha='center', va='top')
        else:
            ax.annotate('SELL', xy=(n-1, sig_c['high']),
                        color='#fca5a5', fontsize=12, fontweight='bold',
                        ha='center', va='bottom')

    colors = ['#60a5fa', '#a855f7', '#f59e0b', '#06b6d4', '#ec4899']
    for i, p in enumerate(positions):
        c = colors[i % len(colors)]
        ax.axhline(p['entry'], color=c, linestyle='--', linewidth=1, alpha=0.7)
        ax.axhline(p['sl'], color='#ef4444', linestyle=':', linewidth=1, alpha=0.7)
        ax.text(n - 0.5, p['entry'], f' #{i+1} {p["entry"]:.2f}',
                color=c, fontsize=7, va='center')

    ax.set_title(f'{symbol} · {CONFIG["TIMEFRAME"]}', color='white', fontsize=12)
    ax.tick_params(colors='#94a3b8', labelsize=8)
    ax.grid(True, color='#ffffff10', linewidth=0.5)
    for spine in ax.spines.values():
        spine.set_color('#ffffff20')
    plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format='png', facecolor='#0a0b0f')
    plt.close(fig)
    buf.seek(0)
    return buf


# ============================================================
# TRADE ENGINE
# ============================================================
class TradeEngine:
    def __init__(self, symbol, timeframe):
        self.symbol = symbol
        self.tf = timeframe
        self.name = f"{symbol}·{timeframe}"

        self.balance = 1000.0
        self.initial_balance = 1000.0
        self.current_lot = CONFIG['LOT_START']
        self.completed_trades = 0
        self.wins = 0
        self.losses = 0
        self.bes = 0
        self.tp1_hits = 0
        self.trail_steps = 0
        self.positions = []          # MULTIPLE POSITIONS
        self.closed = []             # шамъҳои басташуда
        self.forming = None          # шамъи ҷорӣ
        self.last_entry_time = 0
        self.consec_losses = 0
        self.daily_start_balance = 1000.0
        self.daily_date = datetime.now(timezone.utc).date()
        self.paused_until = 0
        self.total_commission = 0.0
        self.total_gross = 0.0

    def update_lot(self):
        bonus = (self.completed_trades // 10) * 0.05
        self.current_lot = min(CONFIG['LOT_START'] + bonus, CONFIG['LOT_CAP'])

    def check_daily_loss(self):
        today = datetime.now(timezone.utc).date()
        if today != self.daily_date:
            self.daily_date = today
            self.daily_start_balance = self.balance
            self.consec_losses = 0
        loss_pct = (self.daily_start_balance - self.balance) / self.daily_start_balance * 100
        return loss_pct >= CONFIG['MAX_DAILY_LOSS']

    def is_paused(self):
        return time.time() < self.paused_until

    def check_engulfing_now(self):
        """Санҷиши шамъи ҷорӣ (дарҳол) бо шамъи қаблии басташуда"""
        if not self.forming or len(self.closed) < 1:
            return None

        cur = self.forming
        p1 = self.closed[-1]

        # Engulfing танҳо 1 шамъ (2 шамъ дар маҷмӯъ)
        bull = (cur['close'] > cur['open'] and
                p1['close'] < p1['open'] and
                cur['open'] <= p1['close'] and
                cur['close'] >= p1['open'])
        bear = (cur['close'] < cur['open'] and
                p1['close'] > p1['open'] and
                cur['open'] >= p1['close'] and
                cur['close'] <= p1['open'])

        if bull:
            return {'type': 'B', 'candles': 1,
                    'ref_low': min(cur['low'], p1['low'])}
        if bear:
            return {'type': 'S', 'candles': 1,
                    'ref_high': max(cur['high'], p1['high'])}
        return None

    async def open_trade(self, client, signal, candle):
        now = time.time()
        if self.is_paused():
            return
        if now - self.last_entry_time < CONFIG['ENTRY_COOLDOWN']:
            return
        if len(self.positions) >= CONFIG['MAX_POSITIONS']:
            return

        if signal['type'] == 'B':
            buf = CONFIG['SL_BUF'] * (candle['close'] / 100000)
            sl = signal['ref_low'] - buf
            sl_dist = candle['close'] - sl
        else:
            buf = CONFIG['SL_BUF'] * (candle['close'] / 100000)
            sl = signal['ref_high'] + buf
            sl_dist = sl - candle['close']

        if sl_dist <= 0:
            return

        lot = self.current_lot
        risk_per_r = lot * 100
        open_comm = lot * candle['close'] * CONFIG['COMM_RATE']
        self.balance -= open_comm
        self.total_commission += open_comm

        try:
            side = SIDE_BUY if signal['type'] == 'B' else SIDE_SELL
            order = await client.create_order(
                symbol=self.symbol, side=side,
                type=ORDER_TYPE_MARKET, quantity=lot
            )
            fill_price = float(order['fills'][0]['price'])

            pos = {
                'time': now, 'symbol': self.symbol,
                'type': signal['type'],
                'entry': fill_price,
                'sl': sl,
                'initial_sl': sl,
                'sl_dist': sl_dist,
                'lot': lot,
                'risk_per_r': risk_per_r,
                'pnl': 0.0,
                'gross': 0.0,
                'commission': open_comm,
                'qty': 1.0,
                'tp1_done': False,
                'be_set': False,
                'lock_r': -1,
                'trail_alerted': set(),
                'max_r': 0.0,
            }
            self.positions.append(pos)
            self.last_entry_time = now

            log.info(f"[{self.name}] ⚡ {signal['type']} {lot} @ ${fill_price:.4f} (pos #{len(self.positions)})")

            img = create_chart(self.symbol, self.closed + [self.forming], signal, self.positions)
            caption = (
                f"⚡ <b>{signal['type']} КУШОДА (Fast)</b> [{self.name}] #{len(self.positions)}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"💰 Entry: <b>${fill_price:.4f}</b>\n"
                f"🛑 SL: <b>${sl:.4f}</b> ({(sl_dist/fill_price*100):.2f}%)\n"
                f"📊 Lot: <b>{lot}</b>\n"
                f"🕯️ Engulf: 1C (2 шамъ)\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"💵 Баланс: <b>${self.balance:.2f}</b>\n"
                f"📂 Кушода: {len(self.positions)}/{CONFIG['MAX_POSITIONS']}"
            )
            await send_photo(img, caption)

        except Exception as e:
            log.error(f"[{self.name}] Order xato: {e}")

    async def manage_positions(self, client, candle):
        for pos in list(self.positions):
            await self._manage_one(client, pos, candle)

    async def _manage_one(self, client, p, candle):
        cur_high = candle['high']
        cur_low = candle['low']

        # Max R (информативӣ)
        if p['type'] == 'B':
            r_now = (cur_high - p['entry']) / p['sl_dist']
        else:
            r_now = (p['entry'] - cur_low) / p['sl_dist']
        if r_now > p['max_r']:
            p['max_r'] = r_now

        # SL тег
        exit_price = None
        exit_r = 0
        if p['type'] == 'B' and cur_low <= p['sl']:
            exit_price = p['sl']
            exit_r = (exit_price - p['entry']) / p['sl_dist']
        elif p['type'] == 'S' and cur_high >= p['sl']:
            exit_price = p['sl']
            exit_r = (p['entry'] - exit_price) / p['sl_dist']

        if exit_price is not None:
            await self._close_position(client, p, exit_price, exit_r)
            return

        # TP1 @ 2R
        if p['max_r'] >= CONFIG['TP1_AT_R'] and not p['tp1_done']:
            p['tp1_done'] = True
            self.tp1_hits += 1

            tp1_exit = (p['entry'] + CONFIG['TP1_AT_R'] * p['sl_dist']
                        if p['type'] == 'B'
                        else p['entry'] - CONFIG['TP1_AT_R'] * p['sl_dist'])
            tp1_gross = CONFIG['TP1_AT_R'] * p['risk_per_r'] * CONFIG['TP1_PCT']
            tp1_size = p['lot'] * CONFIG['TP1_PCT']
            tp1_comm = tp1_size * tp1_exit * CONFIG['COMM_RATE']
            tp1_net = tp1_gross - tp1_comm

            p['pnl'] += tp1_net
            p['gross'] += tp1_gross
            p['commission'] += tp1_comm
            self.balance += tp1_net
            self.total_commission += tp1_comm
            self.total_gross += tp1_gross
            p['qty'] = 1 - CONFIG['TP1_PCT']

            try:
                side = SIDE_SELL if p['type'] == 'B' else SIDE_BUY
                await client.create_order(
                    symbol=self.symbol, side=side,
                    type=ORDER_TYPE_MARKET, quantity=round(tp1_size, 5)
                )
                await send_telegram(
                    f"💰 <b>TP1 (15%) @ 2R</b> [{self.name}]\n"
                    f"Net: <b>${tp1_net:+.2f}</b>\n"
                    f"Қолди: 85%\n"
                    f"💵 Баланс: ${self.balance:.2f}"
                )
            except Exception as e:
                log.error(f"[{self.name}] TP1 xato: {e}")

    async def on_candle_close(self, candle):
        """Вақте шамъ мебаста шавад — SL/BE/Trail нав мешавад"""
        self.closed.append(candle)
        if len(self.closed) > 200:
            self.closed.pop(0)

        buf = CONFIG['SL_BUF'] * (candle['close'] / 100000)

        for p in list(self.positions):
            # ==== 1. Trail SL аз рӯи шамъи басташуда ====
            if p['type'] == 'B':
                candidate = candle['low'] - buf
                if candidate > p['sl']:
                    p['sl'] = candidate
            else:
                candidate = candle['high'] + buf
                if candidate < p['sl']:
                    p['sl'] = candidate

            # ==== 2. BE @ 1:1 (аз рӯи шамъи басташуда) ====
            if p['type'] == 'B':
                closed_r = (candle['high'] - p['entry']) / p['sl_dist']
            else:
                closed_r = (p['entry'] - candle['low']) / p['sl_dist']

            if closed_r >= CONFIG['BE_AT_R'] and not p['be_set']:
                # SL-ро ба Entry мебарор
                if p['type'] == 'B':
                    if p['entry'] > p['sl']:
                        p['sl'] = p['entry']
                else:
                    if p['entry'] < p['sl']:
                        p['sl'] = p['entry']
                p['be_set'] = True
                p['lock_r'] = 0
                await send_telegram(
                    f"🛡️ <b>BE (Break-even) @ 1:1</b> [{self.name}]\n"
                    f"SL ба Entry: <b>${p['entry']:.4f}</b>\n"
                    f"Аз ин пас — <b>хатар нест!</b> 🎉\n"
                    f"💵 Баланс: ${self.balance:.2f}"
                )

            # ==== 3. Step Trail 0.50R ====
            if closed_r >= CONFIG['BE_AT_R']:
                steps = int(closed_r / CONFIG['TRAIL_STEP'])
                lock_r = (steps - 2) * CONFIG['TRAIL_STEP']
                if lock_r >= 0:
                    moved = False
                    if p['type'] == 'B':
                        new_sl = p['entry'] + lock_r * p['sl_dist']
                        if new_sl > p['sl']:
                            p['sl'] = new_sl
                            p['lock_r'] = lock_r
                            moved = True
                    else:
                        new_sl = p['entry'] - lock_r * p['sl_dist']
                        if new_sl < p['sl']:
                            p['sl'] = new_sl
                            p['lock_r'] = lock_r
                            moved = True

                    if moved and lock_r > 0 and lock_r not in p['trail_alerted']:
                        p['trail_alerted'].add(lock_r)
                        self.trail_steps += 1
                        await send_telegram(
                            f"📈 <b>TRAIL: +{lock_r:.1f}R</b> [{self.name}]\n"
                            f"SL нав: <b>${p['sl']:.4f}</b>\n"
                            f"Max R: {closed_r:.2f}R\n"
                            f"💵 Баланс: ${self.balance:.2f}"
                        )

    async def _close_position(self, client, p, exit_price, exit_r):
        try:
            side = SIDE_SELL if p['type'] == 'B' else SIDE_BUY
            await client.create_order(
                symbol=self.symbol, side=side,
                type=ORDER_TYPE_MARKET, quantity=round(p['lot'] * p['qty'], 5)
            )
        except Exception as e:
            log.error(f"[{self.name}] Close xato: {e}")
            return

        gross_pnl = exit_r * p['risk_per_r'] * p['qty']
        close_size = p['lot'] * p['qty']
        close_comm = close_size * exit_price * CONFIG['COMM_RATE']
        net_pnl = gross_pnl - close_comm

        p['gross'] += gross_pnl
        p['commission'] += close_comm
        p['pnl'] += net_pnl
        p['exit'] = exit_price
        p['exit_r'] = exit_r

        self.balance += net_pnl
        self.total_commission += close_comm
        self.total_gross += gross_pnl

        if p['pnl'] > 0.01:
            p['result'] = 'W'; self.wins += 1; self.consec_losses = 0
        elif p['pnl'] < -0.01:
            p['result'] = 'L'; self.losses += 1; self.consec_losses += 1
            if self.consec_losses >= CONFIG['MAX_CONSEC_LOSSES']:
                self.paused_until = time.time() + CONFIG['COOLDOWN_HOURS'] * 3600
        else:
            p['result'] = 'BE'; self.bes += 1

        self.completed_trades += 1
        self.update_lot()
        self.positions.remove(p)

        total_pnl = self.balance - self.initial_balance
        total_pct = total_pnl / self.initial_balance * 100
        wr = (self.wins / self.completed_trades * 100) if self.completed_trades else 0
        emoji = "🟢" if p['result'] == 'W' else "🔴" if p['result'] == 'L' else "⚪"

        await send_telegram(
            f"{emoji} <b>ЁПИЛДИ</b> [{self.name}] {p['type']}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📍 Entry: ${p['entry']:.4f}\n"
            f"🚪 Exit: <b>${exit_price:.4f}</b>\n"
            f"📊 Натиҷа: <b>{exit_r:+.2f}R</b>\n"
            f"💰 Net: <b>${net_pnl:+.2f}</b>\n"
            f"📈 Max R: {p['max_r']:.2f}R\n"
            f"🎯 TP1: {'✅' if p['tp1_done'] else '❌'}\n"
            f"🛡️ BE: {'✅' if p['be_set'] else '❌'}\n"
            f"💸 Комиссия: ${p['commission']:.3f}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Баланс: ${self.balance:.2f}</b>\n"
            f"📊 P&L: <b>${total_pnl:+.2f}</b> ({total_pct:+.2f}%)\n"
            f"🎯 Win Rate: <b>{wr:.1f}%</b>\n"
            f"✅ W: {self.wins} | ❌ L: {self.losses} | ⚪ BE: {self.bes}\n"
            f"🏆 Ҳамагӣ: {self.completed_trades} тиҷорат\n"
            f"📂 Кушода: {len(self.positions)}"
        )


# ============================================================
# ҲАР СИМВОЛ
# ============================================================
async def run_symbol(symbol, client, bsm, engine):
    socket = bsm.kline_socket(symbol, interval=CONFIG['TIMEFRAME'])
    log.info(f"[{engine.name}] Сокет кушода шуд")

    async with socket as stream:
        while True:
            try:
                msg = await stream.recv()
                if msg.get('e') != 'kline':
                    continue
                k = msg['k']

                forming = {
                    'time':   k['t'] // 1000,
                    'open':   float(k['o']),
                    'high':   float(k['h']),
                    'low':    float(k['l']),
                    'close':  float(k['c']),
                    'closed': k['x'],
                }

                # Агар шамъи нав оғоз шавад → шамъи пешина баста мешавад
                if engine.forming and engine.forming['time'] != forming['time']:
                    engine.forming['closed'] = True
                    await engine.on_candle_close(engine.forming)

                engine.forming = forming

                # ⚡ FAST ENTRY — дарҳол, бе интизори басташавӣ
                if not engine.is_paused() and len(engine.closed) >= 1:
                    signal = engine.check_engulfing_now()
                    if signal:
                        await engine.open_trade(client, signal, forming)

                # Идоракунии ҳамаи позицияҳо
                if engine.positions:
                    await engine.manage_positions(client, forming)

            except Exception as e:
                log.error(f"[{engine.name}] Stream xato: {e}")
                await asyncio.sleep(5)


# ============================================================
# АСОСИЙ
# ============================================================
async def main():
    log.info("🚀 Bot v5.0 (Fast Entry + Multi-Position)")
    log.info(f"Symbols: {CONFIG['SYMBOLS']} | TF: {CONFIG['TIMEFRAME']}")

    await send_telegram(
        f"🚀 <b>Engulfing Bot v5.0</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💎 {', '.join(CONFIG['SYMBOLS'])}\n"
        f"⏱️ TF: {CONFIG['TIMEFRAME']}\n"
        f"⚡ Fast Entry (mid-candle)\n"
        f"📂 Max positions: {CONFIG['MAX_POSITIONS']}\n"
        f"🧪 TESTNET: {CONFIG['TESTNET']}"
    )

    client = await AsyncClient.create(
        api_key=CONFIG['API_KEY'],
        api_secret=CONFIG['API_SECRET'],
        testnet=CONFIG['TESTNET']
    )
    bsm = BinanceSocketManager(client)

    tasks = []
    for sym in CONFIG['SYMBOLS']:
        engine = TradeEngine(sym, CONFIG['TIMEFRAME'])
        tasks.append(run_symbol(sym, client, bsm, engine))

    try:
        await asyncio.gather(*tasks)
    finally:
        await client.close_connection()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot тўхтатилди")
