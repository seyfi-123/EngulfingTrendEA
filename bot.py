# ============================================================
# EngulfingTrend Bot v6.2.4 — FULL MULTI-PATTERN
# + 1 SHAM = 1 SIGNAL (priority)
# + KIRISH FAQAT SHAM YOPILGANDA
# + R3B rasmga mos (Morning/Evening Star)
# + BE_AT_R=1.5, TP1_AT_R=1.0
# + KATTA SHAM FILTRI (impuls shamdan keyin kirmaslik)
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

    'ENGULFING': os.getenv('ENGULFING', 'True') == 'True',
    'PINBAR':    os.getenv('PINBAR', 'True') == 'True',
    'BREAKOUT':  os.getenv('BREAKOUT', 'True') == 'True',
    'CONT3BAR':  os.getenv('CONT3BAR', 'True') == 'True',
    'SHRINKING': os.getenv('SHRINKING', 'True') == 'True',
    'REV3BAR':   os.getenv('REV3BAR', 'True') == 'True',
    'LIQSWEEP':  os.getenv('LIQSWEEP', 'True') == 'True',

    'PINBAR_WICK_RATIO': float(os.getenv('PINBAR_WICK_RATIO', '2.0')),
    'BREAKOUT_LOOKBACK': int(os.getenv('BREAKOUT_LOOKBACK', '3')),
    'LIQSWEEP_LOOKBACK': int(os.getenv('LIQSWEEP_LOOKBACK', '10')),

    'BE_AT_R':    float(os.getenv('BE_AT_R', '1.5')),
    'TP1_AT_R':   float(os.getenv('TP1_AT_R', '1.0')),

    'TRAIL_STEP': float(os.getenv('TRAIL_STEP', '2.0')),
    'MAX_TRAIL_R': float(os.getenv('MAX_TRAIL_R', '10.0')),

    'COMM_RATE':  float(os.getenv('COMM_RATE', '0.0005')),

    # === KATTA SHAM FILTRI ===
    'BIG_CANDLE_FILTER': os.getenv('BIG_CANDLE_FILTER', 'True') == 'True',
    'BIG_CANDLE_MULT':   float(os.getenv('BIG_CANDLE_MULT', '2.5')),
    'BIG_CANDLE_LOOKBACK': int(os.getenv('BIG_CANDLE_LOOKBACK', '20')),

    'CHART_CANDLES': int(os.getenv('CHART_CANDLES', '100')),
    'REPORT_HOUR':   int(os.getenv('REPORT_HOUR', '18')),
    'SOCKET_TIMEOUT_MIN':  int(os.getenv('SOCKET_TIMEOUT_MIN', '30')),
    'MAX_DAILY_LOSS_PCT':  float(os.getenv('MAX_DAILY_LOSS_PCT', '0.15')),
    'DIAGNOSTICS_HOUR':    int(os.getenv('DIAGNOSTICS_HOUR', '9')),
}

TELEGRAM_TOKEN   = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()])
log = logging.getLogger(__name__)


class TG:
    def __init__(self, token, chat_id):
        self.bot = Bot(token=token) if token else None
        self.chat_id = chat_id
    async def send(self, msg):
        if not self.bot: return
        try: await self.bot.send_message(chat_id=self.chat_id, text=msg, parse_mode=ParseMode.HTML)
        except Exception as e: log.error(f"TG: {e}")
    async def photo(self, buf, caption=""):
        if not self.bot: return
        try: await self.bot.send_photo(chat_id=self.chat_id, photo=InputFile(buf, filename='chart.png'), caption=caption, parse_mode=ParseMode.HTML)
        except Exception as e: log.error(f"TG photo: {e}")

tg = TG(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)


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
                ax.annotate(text, xy=(i, price), xytext=(i, y), color=color, fontsize=8, fontweight='bold', ha='center', va='top' if is_buy else 'bottom', zorder=7)
            except: pass
        for t in trades[-30:]:
            mark_point(t['time'], t['entry'], t['type'], f"{t.get('pattern','')}-{t.get('part','')}")
        if open_positions:
            for p in open_positions:
                mark_point(p['time'], p['entry'], p['type'], f"{p.get('pattern','')}-{p.get('part','')} (ochiq)")
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
# PATTERN DETECTORS
# ============================================================
def detect_engulfing(cd, idx):
    if idx < 2: return None
    cur = cd[idx]; p1 = cd[idx-1]; p2 = cd[idx-2]
    close_top_2 = max(p1['close'], p2['close'])
    close_bot_2 = min(p1['close'], p2['close'])
    bull2 = (cur['close'] > cur['open'] and p1['close'] < p1['open'] and p2['close'] < p2['open']
             and cur['open'] < close_bot_2 and cur['close'] > close_top_2)
    bear2 = (cur['close'] < cur['open'] and p1['close'] > p1['open'] and p2['close'] > p2['open']
             and cur['open'] > close_top_2 and cur['close'] < close_bot_2)
    if bull2: return {'type': 'B', 'pattern': 'ENG'}
    if bear2: return {'type': 'S', 'pattern': 'ENG'}
    return None

def detect_pinbar(cd, idx):
    if idx < 1: return None
    cur = cd[idx]
    body = abs(cur['close'] - cur['open'])
    if body <= 0: body = 1e-9
    upper_wick = cur['high'] - max(cur['open'], cur['close'])
    lower_wick = min(cur['open'], cur['close']) - cur['low']
    ratio = CONFIG['PINBAR_WICK_RATIO']
    if lower_wick >= body * ratio and lower_wick > upper_wick * 1.5:
        return {'type': 'B', 'pattern': 'PIN'}
    if upper_wick >= body * ratio and upper_wick > lower_wick * 1.5:
        return {'type': 'S', 'pattern': 'PIN'}
    return None

def detect_breakout(cd, idx):
    n = CONFIG['BREAKOUT_LOOKBACK']
    if idx < n: return None
    cur = cd[idx]; prev = cd[idx-n:idx]
    bodies = [abs(c['close'] - c['open']) for c in prev]
    avg_prev_body = sum(bodies) / len(bodies) if bodies else 0
    cur_body = abs(cur['close'] - cur['open'])
    if avg_prev_body <= 0: return None
    if cur_body < avg_prev_body * 2.0: return None
    prev_high = max(c['high'] for c in prev)
    prev_low = min(c['low'] for c in prev)
    if cur['close'] > cur['open'] and cur['close'] > prev_high:
        return {'type': 'B', 'pattern': 'BRK'}
    if cur['close'] < cur['open'] and cur['close'] < prev_low:
        return {'type': 'S', 'pattern': 'BRK'}
    return None

def detect_cont3bar(cd, idx):
    if idx < 2: return None
    cur = cd[idx]; p1 = cd[idx-1]; p2 = cd[idx-2]
    body_p1 = abs(p1['close'] - p1['open'])
    body_p2 = abs(p2['close'] - p2['open'])
    if body_p2 <= 0: return None
    is_pause = body_p1 < body_p2 * 0.6
    bull = (p2['close'] > p2['open'] and is_pause and cur['close'] > cur['open'] and cur['close'] > p2['close'])
    bear = (p2['close'] < p2['open'] and is_pause and cur['close'] < cur['open'] and cur['close'] < p2['close'])
    if bull: return {'type': 'B', 'pattern': 'C3B'}
    if bear: return {'type': 'S', 'pattern': 'C3B'}
    return None

def detect_shrinking(cd, idx):
    if idx < 3: return None
    cur = cd[idx]; p1 = cd[idx-1]; p2 = cd[idx-2]; p3 = cd[idx-3]
    b1 = abs(p1['close'] - p1['open']); b2 = abs(p2['close'] - p2['open']); b3 = abs(p3['close'] - p3['open'])
    is_shrinking = b3 > b2 > b1 and b1 > 0
    cur_body = abs(cur['close'] - cur['open'])
    if not is_shrinking or cur_body <= 0: return None
    if cur_body < b1 * 1.8: return None
    if cur['close'] > cur['open']: return {'type': 'B', 'pattern': 'SHR'}
    if cur['close'] < cur['open']: return {'type': 'S', 'pattern': 'SHR'}
    return None

def detect_rev3bar(cd, idx):
    if idx < 2: return None
    cur = cd[idx]; p1 = cd[idx-1]; p2 = cd[idx-2]
    body_p2 = abs(p2['close'] - p2['open']); body_p1 = abs(p1['close'] - p1['open']); body_cur = abs(cur['close'] - cur['open'])
    if body_p2 <= 0 or body_p1 <= 0: return None
    bull = (p2['close'] < p2['open'] and p1['close'] < p1['open'] and p1['low'] < p2['low']
            and body_p1 >= body_p2 * 0.8 and cur['close'] > cur['open'] and cur['open'] < p1['close']
            and cur['close'] > max(p1['open'], p1['close']) and body_cur >= body_p1 * 0.8)
    bear = (p2['close'] > p2['open'] and p1['close'] > p1['open'] and p1['high'] > p2['high']
            and body_p1 >= body_p2 * 0.8 and cur['close'] < cur['open'] and cur['open'] > p1['close']
            and cur['close'] < min(p1['open'], p1['close']) and body_cur >= body_p1 * 0.8)
    if bull: return {'type': 'B', 'pattern': 'R3B'}
    if bear: return {'type': 'S', 'pattern': 'R3B'}
    return None

def detect_liqsweep(cd, idx):
    n = CONFIG['LIQSWEEP_LOOKBACK']
    if idx < n + 1: return None
    sweep = cd[idx-1]; impulse = cd[idx]
    lookback = cd[idx-1-n: idx-1]
    if not lookback: return None
    prev_high = max(c['high'] for c in lookback)
    prev_low  = min(c['low'] for c in lookback)
    bearish_sweep = (sweep['high'] > prev_high and sweep['close'] < prev_high)
    bearish_impulse = (impulse['close'] < impulse['open'] and impulse['open'] > max(sweep['open'], sweep['close'])
                       and impulse['close'] < min(sweep['open'], sweep['close']))
    if bearish_sweep and bearish_impulse: return {'type': 'S', 'pattern': 'LIQ'}
    bullish_sweep = (sweep['low'] < prev_low and sweep['close'] > prev_low)
    bullish_impulse = (impulse['close'] > impulse['open'] and impulse['open'] < min(sweep['open'], sweep['close'])
                       and impulse['close'] > max(sweep['open'], sweep['close']))
    if bullish_sweep and bullish_impulse: return {'type': 'B', 'pattern': 'LIQ'}
    return None


PATTERN_NAMES = {'ENG':'Engulfing','PIN':'Pinbar','BRK':'Breakout','C3B':'3-Bar Continuation',
                 'SHR':'Shrinking Candles','R3B':'3-Bar Reversal','LIQ':'Liquidity Sweep'}
PATTERN_PRIORITY = ['LIQ','ENG','R3B','PIN','BRK','C3B','SHR']


def check_all_patterns(cd, idx, symbol="?"):
    signals = []
    if CONFIG['ENGULFING']:
        s = detect_engulfing(cd, idx);  signals += [s] if s else []
    if CONFIG['PINBAR']:
        s = detect_pinbar(cd, idx);     signals += [s] if s else []
    if CONFIG['BREAKOUT']:
        s = detect_breakout(cd, idx);   signals += [s] if s else []
    if CONFIG['CONT3BAR']:
        s = detect_cont3bar(cd, idx);   signals += [s] if s else []
    if CONFIG['SHRINKING']:
        s = detect_shrinking(cd, idx);  signals += [s] if s else []
    if CONFIG['REV3BAR']:
        s = detect_rev3bar(cd, idx);    signals += [s] if s else []
    if CONFIG['LIQSWEEP']:
        s = detect_liqsweep(cd, idx);   signals += [s] if s else []

    if signals:
        signals.sort(key=lambda s: PATTERN_PRIORITY.index(s['pattern']) if s['pattern'] in PATTERN_PRIORITY else 999)

    # === KATTA SHAM FILTRI ===
    if CONFIG['BIG_CANDLE_FILTER'] and signals:
        lb = CONFIG['BIG_CANDLE_LOOKBACK']
        if idx >= lb:
            bodies = [abs(cd[i]['close'] - cd[i]['open']) for i in range(idx-lb, idx)]
            avg_body = sum(bodies) / len(bodies) if bodies else 0
            cur_body = abs(cd[idx]['close'] - cd[idx]['open'])
            if avg_body > 0 and cur_body > avg_body * CONFIG['BIG_CANDLE_MULT']:
                ratio = cur_body / avg_body
                log.info(f"🚫 {symbol}: KATTA SHAM filtri — body={cur_body:.4f} "
                         f"({ratio:.1f}x avg) → signal o'tkazildi")
                return []

    return signals


# ============================================================
# TRADE ENGINE
# ============================================================
class Engine:
    def __init__(self, symbol):
        self.symbol = symbol
        self.balance = CONFIG['BALANCE']; self.initial = CONFIG['BALANCE']
        self.completedTrades = 0; self.wins = 0; self.losses = 0; self.bes = 0
        self.tp1_hits = 0; self.trail_steps = 0; self.trail_caps = 0
        self.rt_entries = 0; self.rt_confirmed = 0; self.rt_rejected = 0
        self.positions = []; self.candles = []; self.trades = []
        self.total_comm = 0.0; self.gross_pnl = 0.0
        self.last_candle_time = None
        self.stats_by_pattern = {}
        self.day_start_balance = CONFIG['BALANCE']; self.day_key = None
        self.trading_paused = False
        self.last_signal_time = {}
        self.last_open_candle_time = None
        self.pending_signals = {}

    def calcLot(self, slDist, price):
        if slDist <= 0 or price <= 0: return CONFIG['LOT_MIN']
        risk = self.balance * CONFIG['RISK_PCT']
        lot = risk / slDist
        if lot * slDist < CONFIG['MIN_RISK_USD']: lot = CONFIG['MIN_RISK_USD'] / slDist
        lot = max(CONFIG['LOT_MIN'], min(lot, CONFIG['LOT_MAX']))
        max_lot = (self.balance * 0.95) / price
        return round(min(lot, max_lot), 4)

    def openLocal(self, signal, candle, part='A'):
        cur = candle
        buf = CONFIG['SL_BUF'] * (cur['close'] / 100000)
        if signal['type'] == 'B':
            slPrice = cur['low'] - buf; slDist = cur['close'] - slPrice
        else:
            slPrice = cur['high'] + buf; slDist = slPrice - cur['close']
        if slDist <= 0: return None
        lot = self.calcLot(slDist, cur['close'])
        if lot <= 0: return None
        return {'time': cur['time'], 'type': signal['type'], 'pattern': signal['pattern'],
                'entry': cur['close'], 'sl': slPrice, 'initialSL': slPrice, 'slDist': slDist,
                'lot': lot, 'riskPerR': lot * slDist, 'balance_at_entry': self.balance,
                'beSet': False, 'tp1Done': False, 'lockR': 0, 'gross': 0.0, 'commission': 0.0,
                'part': part, 'is_realtime': False}

    def manageLocal(self, p, candle):
        sl_hit = False; exit_price = 0; exitR = 0
        if p['type'] == 'B' and candle['low'] <= p['sl']:
            sl_hit = True; exit_price = p['sl']; exitR = (exit_price - p['entry']) / p['slDist']
        elif p['type'] == 'S' and candle['high'] >= p['sl']:
            sl_hit = True; exit_price = p['sl']; exitR = (p['entry'] - exit_price) / p['slDist']
        if sl_hit:
            p['exit'] = exit_price; p['exitR'] = exitR
            if p['beSet'] and p.get('lockR', 0) == 0: p['closeReason'] = 'BE'
            elif p.get('lockR', 0) > 0: p['closeReason'] = f"Trail {p['lockR']:.1f}R"
            else: p['closeReason'] = 'SL'
            p['gross'] += exitR * p['riskPerR']; return True
        if p['type'] == 'B': maxR = (candle['high'] - p['entry']) / p['slDist']
        else:               maxR = (p['entry'] - candle['low']) / p['slDist']
        if maxR >= CONFIG['BE_AT_R'] and not p['beSet']:
            p['sl'] = p['entry']; p['beSet'] = True
        if p.get('part') == 'A':
            if maxR >= CONFIG['TP1_AT_R'] and not p.get('tp1Done'):
                if p['type'] == 'B': exit_a = p['entry'] + CONFIG['TP1_AT_R'] * p['slDist']
                else:               exit_a = p['entry'] - CONFIG['TP1_AT_R'] * p['slDist']
                p['exit'] = exit_a; p['exitR'] = CONFIG['TP1_AT_R']
                p['closeReason'] = 'TP1_A (1:1)'
                p['gross'] += CONFIG['TP1_AT_R'] * p['riskPerR']
                p['tp1Done'] = True; self.tp1_hits += 1; return True
            return False
        if p.get('part') == 'B':
            if maxR >= CONFIG['MAX_TRAIL_R']:
                if p['type'] == 'B': exit_b = p['entry'] + CONFIG['MAX_TRAIL_R'] * p['slDist']
                else:               exit_b = p['entry'] - CONFIG['MAX_TRAIL_R'] * p['slDist']
                p['exit'] = exit_b; p['exitR'] = CONFIG['MAX_TRAIL_R']
                p['closeReason'] = f"MAX_CAP {CONFIG['MAX_TRAIL_R']:.0f}R"
                p['gross'] += CONFIG['MAX_TRAIL_R'] * p['riskPerR']
                self.trail_caps += 1; return True
            if maxR >= CONFIG['BE_AT_R']:
                steps = int(maxR / CONFIG['TRAIL_STEP'])
                lockR = (steps - 1) * CONFIG['TRAIL_STEP']
                if lockR > 0:
                    if p['type'] == 'B':
                        newSL = p['entry'] + lockR * p['slDist']
                        if newSL > p['sl']: p['sl'] = newSL; p['lockR'] = lockR; self.trail_steps += 1
                    else:
                        newSL = p['entry'] - lockR * p['slDist']
                        if newSL < p['sl']: p['sl'] = newSL; p['lockR'] = lockR; self.trail_steps += 1
            return False
        return False

    def checkDayReset(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.day_key != today:
            self.day_key = today; self.day_start_balance = self.balance; self.trading_paused = False

    async def openSignal(self, signal, candle, is_realtime=False):
        self.checkDayReset()
        if self.trading_paused: return
        slots_needed = 2 if CONFIG['DUAL_ENTRY'] else 1
        if len(self.positions) + slots_needed > CONFIG['MAX_OPEN_POS']:
            log.info(f"{self.symbol}: joy yo'q"); return
        pattern = signal['pattern']
        if self.last_signal_time.get(pattern) == candle['time']: return
        p_a = self.openLocal(signal, candle, part='A')
        if not p_a: return
        p_a['is_realtime'] = is_realtime
        open_comm_a = p_a['lot'] * p_a['entry'] * CONFIG['COMM_RATE']
        p_a['commission'] += open_comm_a; self.total_comm += open_comm_a
        self.positions.append(p_a)
        self.last_signal_time[pattern] = candle['time']
        if is_realtime: self.rt_entries += 1
        if CONFIG['DUAL_ENTRY']:
            p_b = self.openLocal(signal, candle, part='B')
            if p_b:
                p_b['is_realtime'] = is_realtime
                open_comm_b = p_b['lot'] * p_b['entry'] * CONFIG['COMM_RATE']
                p_b['commission'] += open_comm_b; self.total_comm += open_comm_b
                self.positions.append(p_b)
        action = "BUY" if signal['type'] == 'B' else "SELL"
        emoji = "🟢" if signal['type'] == 'B' else "🔴"
        mode_txt = "⚡ Real-time" if is_realtime else "📊 Sham yopilgan"
        await tg.send(
            f"{emoji} <b>SIGNAL: {action} {self.symbol}</b> 📡\n━━━━━━━━━━━━━━━━━━━\n"
            f"🧩 <b>Pattern:</b> {PATTERN_NAMES.get(pattern, pattern)}\n"
            f"🎯 <b>{mode_txt}</b>\n📊 Narx: <b>${p_a['entry']:.4f}</b>\n"
            f"🛡 SL: ${p_a['sl']:.4f}\n💰 Lot: {p_a['lot']}\n"
            f"📂 Ochiq: <b>{len(self.positions)}/{CONFIG['MAX_OPEN_POS']}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}")
        ch = make_chart(self.candles, self.trades, self.symbol, CONFIG['INTERVAL'],
                        f"· {action} [{pattern}]", open_positions=self.positions)
        if ch: await tg.photo(ch, f"📊 {self.symbol} · {action} [{pattern}]")

    async def closeSignal(self, p):
        close_comm = p['lot'] * p['exit'] * CONFIG['COMM_RATE']
        p['commission'] += close_comm; self.total_comm += close_comm
        net_pnl = p['gross'] - p['commission']
        self.balance += net_pnl; self.gross_pnl += p['gross']
        p['net_pnl'] = net_pnl; self.completedTrades += 1
        if net_pnl > 0.01: p['result'] = 'W'; self.wins += 1
        elif net_pnl < -0.01: p['result'] = 'L'; self.losses += 1
        else: p['result'] = 'BE'; self.bes += 1
        pattern = p.get('pattern', '?')
        st = self.stats_by_pattern.setdefault(pattern, {'wins': 0, 'losses': 0, 'net': 0.0})
        st['net'] += net_pnl
        if net_pnl > 0.01: st['wins'] += 1
        elif net_pnl < -0.01: st['losses'] += 1
        self.trades.append(p); self.positions.remove(p)
        part = p.get('part', '?')
        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        await tg.send(
            f"{emoji} <b>Yopildi [{pattern}-{part}]: {self.symbol}</b>\n━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Exit: ${p['exit']:.4f}\n🎯 R: <b>{p['exitR']:+.2f}R</b>\n"
            f"📌 Sabab: <b>{p.get('closeReason', 'SL')}</b>\n"
            f"💵 Gross: ${p['gross']:+.2f}\n🔻 Komissiya: -${p['commission']:.2f}\n"
            f"💰 <b>Net: ${net_pnl:+.2f}</b>\n📈 Balans: <b>${self.balance:.2f}</b>\n"
            f"📂 Ochiq: {len(self.positions)}/{CONFIG['MAX_OPEN_POS']}")
        ch = make_chart(self.candles, self.trades, self.symbol, CONFIG['INTERVAL'],
                        f"· yopildi [{pattern}-{part}]", open_positions=self.positions)
        if ch: await tg.photo(ch, f"📊 {self.symbol} · yopildi [{pattern}-{part}] ({p['result']})")
        self.checkDayReset()
        day_loss = (self.day_start_balance - self.balance) / self.day_start_balance if self.day_start_balance > 0 else 0
        if day_loss >= CONFIG['MAX_DAILY_LOSS_PCT'] and not self.trading_paused:
            self.trading_paused = True
            await tg.send(f"🛑 <b>{self.symbol}: KUNLIK ZARAR LIMITI</b>\n📉 -{day_loss*100:.1f}%")

    async def handleRealtimeCandle(self, client, candle):
        need = CONFIG['LIQSWEEP_LOOKBACK'] + 2 if CONFIG['LIQSWEEP'] else 4
        if len(self.candles) < need: return
        temp = self.candles[-need:] + [candle]
        signals = check_all_patterns(temp, len(temp) - 1, self.symbol)
        seen_patterns = set()
        for sig in signals:
            pattern = sig['pattern']; seen_patterns.add(pattern)
            pending = self.pending_signals.get(pattern)
            if pending is None:
                self.pending_signals[pattern] = {'signal': sig, 'started_at': time.time(), 'confirm_count': 1}
                continue
            elapsed = time.time() - pending['started_at']
            if pending['signal']['type'] != sig['type']:
                self.rt_rejected += 1; self.pending_signals[pattern] = None; continue
            pending['confirm_count'] += 1
            if elapsed >= CONFIG['CONFIRM_SECONDS'] and pending['confirm_count'] >= 2:
                self.rt_confirmed += 1; self.pending_signals[pattern] = None
        for pattern in list(self.pending_signals.keys()):
            if pattern not in seen_patterns and self.pending_signals.get(pattern):
                self.rt_rejected += 1; self.pending_signals[pattern] = None


ENGINES = {}

async def preload_candles(client, eng, symbol):
    try:
        klines = await client.get_klines(symbol=symbol, interval=CONFIG['INTERVAL'], limit=CONFIG['PRELOAD_CANDLES'])
        candles = [{'time': k[0]//1000, 'open': float(k[1]), 'high': float(k[2]),
                    'low': float(k[3]), 'close': float(k[4]), 'closed': True} for k in klines[:-1]]
        eng.candles = candles
        log.info(f"📥 {symbol}: {len(candles)} ta sham")
    except Exception as e:
        log.error(f"{symbol} preload xato: {e}")


async def worker(client, symbol):
    eng = Engine(symbol); ENGINES[symbol] = eng
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
                    if not msg or msg.get('e') != 'kline': continue
                    eng.last_candle_time = time.time()
                    k = msg['k']
                    candle = {'time': k['t']//1000, 'open': float(k['o']), 'high': float(k['h']),
                              'low': float(k['l']), 'close': float(k['c']), 'closed': k['x']}
                    if CONFIG['REALTIME_ENTRY'] and not candle['closed']:
                        await eng.handleRealtimeCandle(client, candle)
                    if candle['closed']:
                        if eng.candles and eng.candles[-1]['time'] == candle['time']:
                            eng.candles[-1] = candle
                        else:
                            eng.candles.append(candle)
                        if len(eng.candles) > 200: eng.candles.pop(0)
                        eng.pending_signals = {}
                        idx = len(eng.candles) - 1
                        signals = check_all_patterns(eng.candles, idx, eng.symbol)
                        if signals:
                            best_sig = signals[0]
                            already_opened = (eng.last_open_candle_time == candle['time'])
                            if not already_opened:
                                await eng.openSignal(best_sig, candle, is_realtime=False)
                                eng.last_open_candle_time = candle['time']
                    for p in list(eng.positions):
                        if eng.manageLocal(p, candle):
                            await eng.closeSignal(p)
        except Exception as e:
            log.error(f"{symbol} socket xato: {e} — 5s")
            await asyncio.sleep(5)


async def health_check():
    warned = {}
    while True:
        await asyncio.sleep(60); now = time.time()
        for sym, eng in ENGINES.items():
            if eng.last_candle_time is None: continue
            silent = now - eng.last_candle_time
            if silent >= CONFIG['SOCKET_TIMEOUT_MIN'] * 60 and not warned.get(sym):
                warned[sym] = True
                await tg.send(f"⚠️ <b>{sym} socket jim!</b>")
            elif silent < CONFIG['SOCKET_TIMEOUT_MIN'] * 60: warned[sym] = False


async def daily_diagnostics():
    sent = None
    while True:
        now = datetime.now(timezone.utc); key = now.strftime('%Y-%m-%d')
        if now.hour == CONFIG['DIAGNOSTICS_HOUR'] and sent != key:
            sent = key
            if ENGINES:
                txt = f"🔍 <b>DIAGNOSTIKA</b> {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━\n"
                all_patterns = set()
                for e in ENGINES.values(): all_patterns.update(e.stats_by_pattern.keys())
                for pat in sorted(all_patterns):
                    tw = sum(e.stats_by_pattern.get(pat, {}).get('wins', 0) for e in ENGINES.values())
                    tl = sum(e.stats_by_pattern.get(pat, {}).get('losses', 0) for e in ENGINES.values())
                    tn = sum(e.stats_by_pattern.get(pat, {}).get('net', 0.0) for e in ENGINES.values())
                    wr = tw/(tw+tl)*100 if (tw+tl) > 0 else 0
                    txt += f"🧩 {PATTERN_NAMES.get(pat, pat)}: ✅{tw} ❌{tl} (WR {wr:.1f}%) | ${tn:+.2f}\n"
                for s, e in ENGINES.items():
                    net = e.balance - e.initial
                    txt += f"{'🟢' if net >= 0 else '🔴'} {s}: {net:+.2f}$\n"
                await tg.send(txt)
        await asyncio.sleep(60)


async def daily_report():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == CONFIG['REPORT_HOUR'] and now.minute < 1:
            tb = sum(e.balance for e in ENGINES.values())
            ti = sum(e.initial for e in ENGINES.values())
            tw = sum(e.wins for e in ENGINES.values()); tl = sum(e.losses for e in ENGINES.values())
            td = sum(e.wins+e.losses+e.bes for e in ENGINES.values())
            wr = tw/td*100 if td > 0 else 0
            pct = (tb-ti)/ti*100 if ti else 0
            txt = f"📊 <b>KUNLIK HISOBOT</b>\n━━━━━━━━━━━━━━━━━━\n"
            for s, e in ENGINES.items():
                sp = (e.balance-e.initial)/e.initial*100
                em = "🟢" if e.balance >= e.initial else "🔴"
                txt += f"{em} <b>{s}</b>: ${e.balance:.2f} ({sp:+.2f}%)\n"
            txt += f"━━━━━━━━━━━━━━━━━━\nJAMI: WR {wr:.1f}% | ${tb:.2f} ({pct:+.2f}%)"
            await tg.send(txt); await asyncio.sleep(60)
        await asyncio.sleep(30)


async def main():
    log.info("🚀 Bot v6.2.4 — Katta sham filtri")
    log.info(f"BE_AT_R={CONFIG['BE_AT_R']} | TP1_AT_R={CONFIG['TP1_AT_R']} | "
             f"BIG_CANDLE_FILTER={CONFIG['BIG_CANDLE_FILTER']} ({CONFIG['BIG_CANDLE_MULT']}x)")
    await tg.send(f"🚀 <b>Bot v6.2.4 — Katta sham filtri</b>\n"
                  f"🎯 BE={CONFIG['BE_AT_R']}R | TP1={CONFIG['TP1_AT_R']}R\n"
                  f"🚫 Katta sham: {CONFIG['BIG_CANDLE_MULT']}x > o'rtacha → o'tkaziladi")
    client = await AsyncClient.create()
    tasks = [asyncio.create_task(worker(client, s)) for s in CONFIG['SYMBOLS']]
    tasks.append(asyncio.create_task(daily_report()))
    tasks.append(asyncio.create_task(health_check()))
    tasks.append(asyncio.create_task(daily_diagnostics()))
    try: await asyncio.gather(*tasks)
    except Exception as e: log.error(f"Main: {e}")
    finally: await client.close_connection()


if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Bot to'xtatildi")