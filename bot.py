# ============================================================
# EngulfingTrend Bot v8.0.0 — VOTING SYSTEM + MEMORY + PYRAMID
# 2 ovoz → A+B | 3 ovoz → A+B+C+D
# ============================================================
import os
import asyncio
import logging
import time
import io
import sqlite3
import aiohttp
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
    'INTERVAL':   os.getenv('INTERVAL', '5m'),
    'BALANCE':    float(os.getenv('BALANCE', '1000')),
    'RISK_PCT':   float(os.getenv('RISK_PCT', '0.02')),
    'LOT_MIN':    float(os.getenv('LOT_MIN', '0.001')),
    'LOT_MAX':    float(os.getenv('LOT_MAX', '2.0')),
    'MIN_RISK_USD': float(os.getenv('MIN_RISK_USD', '2.0')),
    'MAX_OPEN_POS': int(os.getenv('MAX_OPEN_POS', '20')),
    'PRELOAD_CANDLES': int(os.getenv('PRELOAD_CANDLES', '250')),
    'SL_BUF':     float(os.getenv('SL_BUF', '10')),

    'TECH_BOT': os.getenv('TECH_BOT', 'True') == 'True',
    'FUND_BOT': os.getenv('FUND_BOT', 'True') == 'True',

    # === Trend voting ===
    'TREND_FILTER':   os.getenv('TREND_FILTER', 'True') == 'True',
    'TREND_LOOKBACK': int(os.getenv('TREND_LOOKBACK', '20')),

    # === Memory ===
    'MEMORY_ENABLED':   os.getenv('MEMORY_ENABLED', 'True') == 'True',
    'MEMORY_MIN_TRADES': int(os.getenv('MEMORY_MIN_TRADES', '10')),
    'MEMORY_MIN_WR':    float(os.getenv('MEMORY_MIN_WR', '0.30')),
    'MEMORY_DB_PATH':   os.getenv('MEMORY_DB_PATH', '/app/bot_memory.db'),

    # === 2C Engulfing (o'zgarmas) ===
    'BE_AT_R':     float(os.getenv('BE_AT_R', '1.0')),
    'TP1_AT_R':    float(os.getenv('TP1_AT_R', '1.0')),
    'TRAIL_STEP':  float(os.getenv('TRAIL_STEP', '2.0')),
    'MAX_TRAIL_R': float(os.getenv('MAX_TRAIL_R', '10.0')),
    'COMM_RATE':   float(os.getenv('COMM_RATE', '0.0005')),

    # === Fundamental (faqat xabar) ===
    'NEWS_CHECK_SEC':         int(os.getenv('NEWS_CHECK_SEC', '300')),

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
        self.last_send = 0

    async def send(self, msg):
        if not self.bot: return
        now = time.time()
        if now - self.last_send < 1:
            await asyncio.sleep(1)
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=msg, parse_mode=ParseMode.HTML)
            self.last_send = time.time()
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
# 🧠 MEMORY
# ============================================================
class BotMemory:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, setup_key TEXT, trend TEXT, votes INTEGER,
                signal_type TEXT, part TEXT, result TEXT,
                exit_r REAL, net_pnl REAL, reason TEXT, ts INTEGER
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS patterns (
                pattern_key TEXT PRIMARY KEY,
                wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, be INTEGER DEFAULT 0,
                total_net REAL DEFAULT 0.0, blocked INTEGER DEFAULT 0,
                last_update INTEGER
            )''')
            conn.commit(); conn.close()
            log.info(f"🧠 Memory DB: {self.db_path}")
        except Exception as e:
            log.error(f"Memory init xato: {e}")

    def record(self, symbol, setup_key, trend, votes, signal_type, part, result, exit_r, net_pnl, reason):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''INSERT INTO trades
                (symbol, setup_key, trend, votes, signal_type, part, result, exit_r, net_pnl, reason, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (symbol, setup_key, trend, votes, signal_type, part, result, exit_r, net_pnl, reason, int(time.time())))

            pk = f"{setup_key}|{trend}|V{votes}"
            c.execute('SELECT wins, losses, be, total_net FROM patterns WHERE pattern_key=?', (pk,))
            row = c.fetchone()
            if row:
                w, l, be, tot = row
                if result == 'W': w += 1
                elif result == 'L': l += 1
                else: be += 1
                tot += net_pnl
                c.execute('''UPDATE patterns SET wins=?, losses=?, be=?, total_net=?, last_update=?
                             WHERE pattern_key=?''', (w, l, be, tot, int(time.time()), pk))
            else:
                w = 1 if result == 'W' else 0
                l = 1 if result == 'L' else 0
                be = 1 if result == 'BE' else 0
                c.execute('''INSERT INTO patterns VALUES (?, ?, ?, ?, ?, 0, ?)''',
                          (pk, w, l, be, net_pnl, int(time.time())))
            conn.commit(); conn.close()
        except Exception as e:
            log.error(f"Memory record xato: {e}")

    def get_stats(self, setup_key, trend, votes):
        try:
            pk = f"{setup_key}|{trend}|V{votes}"
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('SELECT wins, losses, be, total_net FROM patterns WHERE pattern_key=?', (pk,))
            row = c.fetchone(); conn.close()
            if row:
                return {'wins': row[0], 'losses': row[1], 'be': row[2], 'total_net': row[3]}
            return None
        except: return None

    def should_block(self, setup_key, trend, votes):
        if not CONFIG['MEMORY_ENABLED']:
            return False, None
        stats = self.get_stats(setup_key, trend, votes)
        if not stats: return False, None
        total = stats['wins'] + stats['losses']
        if total < CONFIG['MEMORY_MIN_TRADES']:
            return False, stats
        wr = stats['wins'] / total if total > 0 else 0
        if wr < CONFIG['MEMORY_MIN_WR'] and stats['total_net'] < 0:
            return True, stats
        return False, stats

    def all_stats(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('SELECT pattern_key, wins, losses, total_net FROM patterns ORDER BY total_net DESC LIMIT 10')
            rows = c.fetchall(); conn.close()
            return rows
        except: return []


memory = BotMemory(CONFIG['MEMORY_DB_PATH'])


# ============================================================
# 📰 FUNDAMENTAL (faqat xabar)
# ============================================================
class NewsBot:
    def __init__(self):
        self.last_check = 0
        self.seen_news = set()
        self.high_impact_until = 0
        self.last_text = ""
        self.affected = []

    async def fetch_news(self):
        try:
            url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
            params = {"type": 1, "pageNo": 1, "pageSize": 20}
            headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status != 200: return []
                    data = await resp.json()
                    lst = []
                    for cat in data.get('data', {}).get('catalogs', []):
                        for art in cat.get('articles', []):
                            lst.append({'id': art.get('id'), 'title': art.get('title', ''),
                                        'release_date': art.get('releaseDate', 0)})
                    return lst
        except: return []

    def is_high(self, title):
        t = title.lower()
        kws = ['listing', 'delisting', 'halt', 'suspend', 'resume', 'hard fork',
               'upgrade', 'mainnet', 'launch', 'halving', 'airdrop', 'partnership',
               'sec', 'regulation', 'lawsuit', 'ban']
        return any(k in t for k in kws)

    def which_symbols(self, title):
        tu = title.upper()
        aff = []
        for sym in CONFIG['SYMBOLS']:
            base = sym.replace('USDT', '')
            if base in tu: aff.append(sym)
        return aff

    async def check(self):
        now = time.time()
        if now - self.last_check < CONFIG['NEWS_CHECK_SEC']: return
        self.last_check = now
        news = await self.fetch_news()
        for n in news:
            nid = n['id']
            if nid in self.seen_news: continue
            self.seen_news.add(nid)
            if self.is_high(n['title']):
                aff = self.which_symbols(n['title'])
                self.high_impact_until = now + 7200
                self.last_text = n['title']
                self.affected = aff
                sym_str = ', '.join(aff) if aff else 'BOZOR'
                await tg.send(
                    f"📰 <b>FUNDAMENTAL YANGILIK</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"🎯 Ta'sir: <b>{sym_str}</b>\n"
                    f"📌 {n['title'][:200]}\n"
                    f"⚠️ <i>Faqat xabar — savdoga ta'sir qilmaydi</i>"
                )

    def get_context(self):
        if time.time() >= self.high_impact_until: return ""
        rem = int((self.high_impact_until - time.time()) / 60)
        return f"\n📰 Fundamental: {rem}m volatillik"


news_bot = NewsBot()


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

        idx_list = list(df.index)
        def mark(t, price, side, extra=""):
            try:
                tt = datetime.utcfromtimestamp(t)
                if tt not in idx_list: return
                i = idx_list.index(tt)
                is_buy = side == 'B'
                col = '#10b981' if is_buy else '#ef4444'
                mrk = '^' if is_buy else 'v'
                txt = ("BUY" if is_buy else "SELL") + (f" {extra}" if extra else "")
                yo = (df['high'].max() - df['low'].min()) * 0.02
                y = price - yo if is_buy else price + yo
                ax.scatter(i, price, color=col, marker=mrk, s=160, zorder=6, edgecolors='white', linewidths=0.5)
                ax.annotate(txt, xy=(i, price), xytext=(i, y), color=col,
                            fontsize=8, fontweight='bold', ha='center',
                            va='top' if is_buy else 'bottom', zorder=7)
            except: pass

        for t in trades[-30:]:
            mark(t['time'], t['entry'], t['type'], f"{t.get('part','')}")
        if open_positions:
            for p in open_positions:
                mark(p['time'], p['entry'], p['type'], f"{p.get('part','')}")

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
        self.trend_blocks = 0; self.memory_blocks = 0
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
        self.current_trend = 'WEAK'
        self.trend_votes = {'up': 0, 'down': 0}
        self.trend_methods = {'HH/HL': '', 'S/R': '', 'BigC': ''}

    def calcLot(self, slDist, price):
        if slDist <= 0 or price <= 0: return CONFIG['LOT_MIN']
        risk = self.balance * CONFIG['RISK_PCT']
        lot = risk / slDist
        if lot * slDist < CONFIG['MIN_RISK_USD']:
            lot = CONFIG['MIN_RISK_USD'] / slDist
        lot = max(CONFIG['LOT_MIN'], min(lot, CONFIG['LOT_MAX']))
        max_lot = (self.balance * 0.95) / price
        return round(min(lot, max_lot), 4)

    # ---------- Trend 3 usul ----------
    def _t_hh_hl(self):
        if len(self.candles) < 10: return 'WEAK'
        lb = CONFIG['TREND_LOOKBACK']
        h = [c['high'] for c in self.candles[-lb:]]
        l = [c['low'] for c in self.candles[-lb:]]
        if h[-1] > h[-3] > h[-5] > h[-7] and l[-1] > l[-3] > l[-5] > l[-7]:
            return 'STRONG_UP'
        if h[-1] < h[-3] < h[-5] < h[-7] and l[-1] < l[-3] < l[-5] < l[-7]:
            return 'STRONG_DOWN'
        return 'WEAK'

    def _t_sr(self):
        if len(self.candles) < 20: return 'WEAK'
        lb = CONFIG['TREND_LOOKBACK']
        f = self.candles[-lb:-lb//2]; s = self.candles[-lb//2:]
        if not f or not s: return 'WEAK'
        fh = max(c['high'] for c in f); fl = min(c['low'] for c in f)
        sh = max(c['high'] for c in s); sl = min(c['low'] for c in s)
        if sh > fh and sl > fl: return 'STRONG_UP'
        if sh < fh and sl < fl: return 'STRONG_DOWN'
        return 'WEAK'

    def _t_big(self):
        if len(self.candles) < 20: return 'WEAK'
        lb = CONFIG['TREND_LOOKBACK']
        recent = self.candles[-5:]
        all_c = self.candles[-lb:]
        if not all_c: return 'WEAK'
        avg = sum(abs(c['close']-c['open']) for c in all_c) / len(all_c)
        if avg <= 0: return 'WEAK'
        bg = sum(1 for c in recent if c['close'] > c['open'] and abs(c['close']-c['open']) > avg*1.5)
        br = sum(1 for c in recent if c['close'] < c['open'] and abs(c['close']-c['open']) > avg*1.5)
        if bg >= 3: return 'STRONG_UP'
        if br >= 3: return 'STRONG_DOWN'
        return 'WEAK'

    def detect_trend(self):
        """3 usul → ovozlar"""
        self.trend_methods['HH/HL'] = self._t_hh_hl()
        self.trend_methods['S/R']   = self._t_sr()
        self.trend_methods['BigC']  = self._t_big()
        vu = sum(1 for v in self.trend_methods.values() if v == 'STRONG_UP')
        vd = sum(1 for v in self.trend_methods.values() if v == 'STRONG_DOWN')
        self.trend_votes = {'up': vu, 'down': vd}
        if vu >= 2 and vu > vd: return 'STRONG_UP', vu
        if vd >= 2 and vd > vu: return 'STRONG_DOWN', vd
        return 'WEAK', max(vu, vd)

    # ---------- 2C Engulfing (o'zgarmas) ----------
    def checkEngulfing(self, cd, idx):
        if idx < 2: return None
        cur, p1, p2 = cd[idx], cd[idx-1], cd[idx-2]
        p1t, p1b = max(p1['open'], p1['close']), min(p1['open'], p1['close'])
        p2t, p2b = max(p2['open'], p2['close']), min(p2['open'], p2['close'])
        bt = max(p1t, p2t); bb = min(p1b, p2b)
        bull = (cur['close'] > cur['open'] and p1['close'] < p1['open'] and p2['close'] < p2['open']
                and cur['open'] < bb and cur['close'] > bt)
        bear = (cur['close'] < cur['open'] and p1['close'] > p1['open'] and p2['close'] > p2['open']
                and cur['open'] > bt and cur['close'] < bb)
        if bull: return {'type': 'B', 'candles': 2}
        if bear: return {'type': 'S', 'candles': 2}
        return None

    def openLocal(self, signal, candle, part='A'):
        cur = candle
        buf = CONFIG['SL_BUF'] * (cur['close'] / 100000)
        if signal['type'] == 'B':
            slP = cur['low'] - buf; slD = cur['close'] - slP
        else:
            slP = cur['high'] + buf; slD = slP - cur['close']
        if slD <= 0: return None
        lot = self.calcLot(slD, cur['close'])
        if lot <= 0: return None
        return {
            'time': cur['time'], 'type': signal['type'],
            'engulfCandles': signal['candles'],
            'entry': cur['close'], 'sl': slP, 'slDist': slD,
            'lot': lot, 'riskPerR': lot * slD,
            'beSet': False, 'tp1Done': False, 'lockR': 0,
            'gross': 0.0, 'commission': 0.0, 'part': part,
            'trend_at_entry': self.current_trend,
            'votes_at_entry': max(self.trend_votes['up'], self.trend_votes['down']),
        }

    def manageLocal(self, p, candle):
        sl_hit = False; ep = 0; eR = 0
        if p['type'] == 'B' and candle['low'] <= p['sl']:
            sl_hit = True; ep = p['sl']; eR = (ep - p['entry']) / p['slDist']
        elif p['type'] == 'S' and candle['high'] >= p['sl']:
            sl_hit = True; ep = p['sl']; eR = (p['entry'] - ep) / p['slDist']
        if sl_hit:
            p['exit'] = ep; p['exitR'] = eR
            if p['beSet'] and p.get('lockR', 0) == 0: p['closeReason'] = 'BE'
            elif p.get('lockR', 0) > 0: p['closeReason'] = f"Trail {p['lockR']:.1f}R"
            else: p['closeReason'] = 'SL'
            p['gross'] += eR * p['riskPerR']
            return True

        if p['type'] == 'B': maxR = (candle['high'] - p['entry']) / p['slDist']
        else: maxR = (p['entry'] - candle['low']) / p['slDist']

        if maxR >= CONFIG['BE_AT_R'] and not p['beSet']:
            p['sl'] = p['entry']; p['beSet'] = True

        if p.get('part') == 'A':
            if maxR >= CONFIG['TP1_AT_R'] and not p.get('tp1Done'):
                ep = p['entry'] + CONFIG['TP1_AT_R'] * p['slDist'] if p['type'] == 'B' \
                     else p['entry'] - CONFIG['TP1_AT_R'] * p['slDist']
                p['exit'] = ep; p['exitR'] = CONFIG['TP1_AT_R']
                p['closeReason'] = 'TP1_A (1:1)'
                p['gross'] += CONFIG['TP1_AT_R'] * p['riskPerR']
                p['tp1Done'] = True; self.tp1_hits += 1
                return True
            return False

        if p.get('part') in ['B', 'C', 'D']:
            if maxR >= CONFIG['MAX_TRAIL_R']:
                ep = p['entry'] + CONFIG['MAX_TRAIL_R'] * p['slDist'] if p['type'] == 'B' \
                     else p['entry'] - CONFIG['MAX_TRAIL_R'] * p['slDist']
                p['exit'] = ep; p['exitR'] = CONFIG['MAX_TRAIL_R']
                p['closeReason'] = f"MAX_CAP {CONFIG['MAX_TRAIL_R']:.0f}R"
                p['gross'] += CONFIG['MAX_TRAIL_R'] * p['riskPerR']
                self.trail_caps += 1
                return True
            if maxR >= CONFIG['BE_AT_R']:
                steps = int(maxR / CONFIG['TRAIL_STEP'])
                lockR = (steps - 1) * CONFIG['TRAIL_STEP']
                if lockR > 0:
                    if p['type'] == 'B':
                        nSL = p['entry'] + lockR * p['slDist']
                        if nSL > p['sl']:
                            p['sl'] = nSL; p['lockR'] = lockR; self.trail_steps += 1
                    else:
                        nSL = p['entry'] - lockR * p['slDist']
                        if nSL < p['sl']:
                            p['sl'] = nSL; p['lockR'] = lockR; self.trail_steps += 1
            return False
        return False

    def checkDayReset(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.day_key != today:
            self.day_key = today
            self.day_start_balance = self.balance
            self.trading_paused = False

    # ----------------------------------------------------------
    async def openSignal(self, signal, candle):
        self.checkDayReset()
        if self.trading_paused: return

        setup_key = f"2C"

        # ============ 1. TREND (ovoz bilan) ============
        trend, votes = self.detect_trend()
        self.current_trend = trend

        if CONFIG['TREND_FILTER']:
            if trend == 'WEAK' or votes < 2:
                self.trend_blocks += 1
                log.info(f"⛔ {self.symbol}: votes {votes}/3, trend {trend} — o'tkazildi")
                return
            if trend == 'STRONG_UP' and signal['type'] != 'B':
                self.trend_blocks += 1
                log.info(f"⛔ {self.symbol}: STRONG_UP, SELL o'tkazildi")
                return
            if trend == 'STRONG_DOWN' and signal['type'] != 'S':
                self.trend_blocks += 1
                log.info(f"⛔ {self.symbol}: STRONG_DOWN, BUY o'tkazildi")
                return

        # ============ 2. MEMORY ============
        blocked, stats = memory.should_block(setup_key, trend, votes)
        if blocked:
            self.memory_blocks += 1
            log.info(f"🧠 {self.symbol}: MEMORY blokladi {setup_key}|{trend}|V{votes}")
            return

        # ============ 3. OVOZGA QARAB POZITSIYALAR ============
        if votes >= 3:
            parts = ['A', 'B', 'C', 'D']  # 4 ta pozitsiya
            levels = 4
        else:
            parts = ['A', 'B']            # 2 ta pozitsiya
            levels = 2

        if len(self.positions) + levels > CONFIG['MAX_OPEN_POS']:
            log.info(f"{self.symbol}: joy yo'q ({len(self.positions)}/{CONFIG['MAX_OPEN_POS']}, kerak {levels})")
            return
        if self.last_signal_time == candle['time']:
            return

        opened = []
        for part in parts:
            p = self.openLocal(signal, candle, part=part)
            if not p: continue
            oc = p['lot'] * p['entry'] * CONFIG['COMM_RATE']
            p['commission'] += oc
            self.total_comm += oc
            self.positions.append(p)
            opened.append(part)

        if not opened: return
        self.last_signal_time = candle['time']

        action = "BUY" if signal['type'] == 'B' else "SELL"
        emoji = "🟢" if signal['type'] == 'B' else "🔴"
        trend_emoji = "📈" if trend == 'STRONG_UP' else "📉"
        fund_note = news_bot.get_context()

        mem_line = ""
        if stats:
            tot = stats['wins'] + stats['losses']
            wr = (stats['wins']/tot*100) if tot > 0 else 0
            mem_line = f"\n🧠 Memory: {wr:.0f}% WR ({stats['wins']}W/{stats['losses']}L)"

        await tg.send(
            f"{emoji} <b>SIGNAL: {action} {self.symbol}</b> 📡\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{trend_emoji} <b>{trend}</b> | 🗳 <b>{votes}/3 ovoz</b>{mem_line}\n"
            f"📊 Narx: <b>${p['entry']:.4f}</b>\n"
            f"🛡 SL: ${p['sl']:.4f}\n"
            f"💰 Lot (har biri): {p['lot']}\n"
            f"🎯 <b>Pozitsiyalar: {' + '.join(opened)} ({levels} ta)</b>\n"
            f"📂 Ochiq: <b>{len(self.positions)}/{CONFIG['MAX_OPEN_POS']}</b>{fund_note}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )

        ch = make_chart(self.candles, self.trades, self.symbol,
                        CONFIG['INTERVAL'], f"· {action}",
                        open_positions=self.positions)
        if ch:
            await tg.photo(ch, f"📊 {self.symbol} · {action}")

    # ----------------------------------------------------------
    async def closeSignal(self, p):
        cc = p['lot'] * p['exit'] * CONFIG['COMM_RATE']
        p['commission'] += cc
        self.total_comm += cc
        net = p['gross'] - p['commission']
        self.balance += net
        self.gross_pnl += p['gross']
        p['net_pnl'] = net
        self.completedTrades += 1

        if net > 0.01: p['result'] = 'W'; self.wins += 1
        elif net < -0.01: p['result'] = 'L'; self.losses += 1
        else: p['result'] = 'BE'; self.bes += 1

        self.stats_2c['net'] += net
        if net > 0.01: self.stats_2c['wins'] += 1
        elif net < -0.01: self.stats_2c['losses'] += 1

        self.trades.append(p)
        self.positions.remove(p)

        # Memory
        memory.record(self.symbol, "2C", p.get('trend_at_entry', 'WEAK'),
                      p.get('votes_at_entry', 0), p['type'],
                      p.get('part', 'A'), p['result'], p['exitR'], net,
                      p.get('closeReason', 'SL'))

        log.info(f"{self.symbol} [{p.get('part')}]: {p['exitR']:+.2f}R | net ${net:+.2f} | balans ${self.balance:.2f}")

        emoji = "✅" if p['result'] == 'W' else "❌" if p['result'] == 'L' else "⚪"
        await tg.send(
            f"{emoji} <b>Yopildi [{p.get('part')}]: {self.symbol}</b>\n"
            f"📊 ${p['exit']:.4f} | <b>{p['exitR']:+.2f}R</b> | {p.get('closeReason','SL')}\n"
            f"💰 Net: <b>${net:+.2f}</b> | Balans: <b>${self.balance:.2f}</b>"
        )

        self.checkDayReset()
        dl = (self.day_start_balance - self.balance) / self.day_start_balance if self.day_start_balance > 0 else 0
        if dl >= CONFIG['MAX_DAILY_LOSS_PCT'] and not self.trading_paused:
            self.trading_paused = True
            await tg.send(f"🛑 <b>{self.symbol}: KUNLIK ZARAR LIMITI</b>\n📉 -{dl*100:.1f}%")


# ============================================================
# GLOBAL
# ============================================================
ENGINES = {}


async def preload_candles(client, eng, symbol):
    try:
        klines = await client.get_klines(symbol=symbol, interval=CONFIG['INTERVAL'], limit=CONFIG['PRELOAD_CANDLES'])
        eng.candles = [{'time': k[0]//1000, 'open': float(k[1]),
                        'high': float(k[2]), 'low': float(k[3]),
                        'close': float(k[4]), 'closed': True} for k in klines[:-1]]
        log.info(f"📥 {symbol}: {len(eng.candles)} ta sham")
    except Exception as e:
        log.error(f"{symbol} preload: {e}")


async def worker(client, symbol):
    eng = Engine(symbol)
    ENGINES[symbol] = eng
    log.info(f"🔵 {symbol} | MAX {CONFIG['MAX_OPEN_POS']} | TREND={CONFIG['TREND_FILTER']}")
    await preload_candles(client, eng, symbol)

    bsm = BinanceSocketManager(client)
    while True:
        try:
            socket = bsm.kline_socket(symbol=symbol, interval=CONFIG['INTERVAL'])
            async with socket as stream:
                log.info(f"🟢 {symbol} socket")
                while True:
                    msg = await stream.recv()
                    if not msg or msg.get('e') != 'kline': continue
                    eng.last_candle_time = time.time()
                    k = msg['k']
                    candle = {'time': k['t']//1000, 'open': float(k['o']),
                              'high': float(k['h']), 'low': float(k['l']),
                              'close': float(k['c']), 'closed': k['x']}
                    if candle['closed']:
                        if eng.candles and eng.candles[-1]['time'] == candle['time']:
                            eng.candles[-1] = candle
                        else:
                            eng.candles.append(candle)
                        if len(eng.candles) > 300: eng.candles.pop(0)
                        idx = len(eng.candles) - 1
                        sig = eng.checkEngulfing(eng.candles, idx)
                        if sig: await eng.openSignal(sig, candle)
                    for p in list(eng.positions):
                        if eng.manageLocal(p, candle): await eng.closeSignal(p)
        except Exception as e:
            log.error(f"{symbol} socket xato: {e}")
            await asyncio.sleep(5)


async def news_worker():
    if not CONFIG['FUND_BOT']: return
    log.info("📰 News bot (faqat xabar)")
    while True:
        try: await news_bot.check()
        except: pass
        await asyncio.sleep(30)


async def health_check():
    warned = {}
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for sym, eng in ENGINES.items():
            if eng.last_candle_time is None: continue
            silent = now - eng.last_candle_time
            if silent >= CONFIG['SOCKET_TIMEOUT_MIN']*60 and not warned.get(sym):
                warned[sym] = True
                await tg.send(f"⚠️ <b>{sym} socket jim!</b> {int(silent//60)} daqiqa")
            elif silent < CONFIG['SOCKET_TIMEOUT_MIN']*60:
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
                tb = sum(e.trend_blocks for e in ENGINES.values())
                mb = sum(e.memory_blocks for e in ENGINES.values())
                wr = tw/(tw+tl)*100 if (tw+tl) > 0 else 0
                txt = f"🔍 <b>DIAGNOSTIKA</b> {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━\n"
                txt += f"2C: ✅{tw} ❌{tl} (WR {wr:.1f}%) | ${tn:+.2f}\n"
                txt += f"⛔ Trend blok: {tb} | 🧠 Memory blok: {mb}\n\n"
                ms = memory.all_stats()
                if ms:
                    txt += "🧠 <b>Memory:</b>\n"
                    for pk, w, l, net in ms[:5]:
                        tot = w+l; wp = (w/tot*100) if tot > 0 else 0
                        em = "🟢" if net >= 0 else "🔴"
                        txt += f"{em} {pk}: {wp:.0f}% ({w}W/{l}L) ${net:+.2f}\n"
                    txt += "\n"
                for s, e in ENGINES.items():
                    net = e.balance - e.initial
                    em = "🟢" if net >= 0 else "🔴"
                    txt += f"{em} {s}: {net:+.2f}$ | {e.current_trend} | Ochiq: {len(e.positions)}\n"
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
            wr = tw/td*100 if td > 0 else 0
            pct = (tb-ti)/ti*100 if ti else 0
            txt = f"📊 <b>KUNLIK HISOBOT</b> {now.strftime('%d.%m.%Y')}\n━━━━━━━━━━━━━━━━━━\n"
            for s, e in ENGINES.items():
                sp = (e.balance-e.initial)/e.initial*100
                em = "🟢" if e.balance >= e.initial else "🔴"
                txt += f"\n{em} <b>{s}</b>: ${e.balance:.2f} ({sp:+.2f}%)\n"
            txt += f"\n━━━━━━━━━━━━━━━━━━\nJAMI: WR {wr:.1f}% | ${tb:.2f} ({pct:+.2f}%)"
            await tg.send(txt)
            await asyncio.sleep(60)
        await asyncio.sleep(30)


async def main():
    log.info("🚀 Bot v8.0.0 — VOTING + MEMORY + PYRAMID")
    log.info(f"Symbols: {CONFIG['SYMBOLS']} | TF: {CONFIG['INTERVAL']}")

    await tg.send(
        f"🚀 <b>Engulfing Bot v8.0.0</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 2C Engulfing (o'zgarmas)\n"
        f"📊 Trend: HH/HL + S/R + BigCandle\n"
        f"🗳 <b>2 ovoz → A+B (2 pozitsiya)</b>\n"
        f"🗳 <b>3 ovoz → A+B+C+D (4 pozitsiya)</b>\n"
        f"🧠 Memory: yomon setup'larni bloklaydi\n"
        f"📰 Fundamental: faqat xabar\n"
        f"📊 {', '.join(CONFIG['SYMBOLS'])}\n"
        f"⏱ TF: {CONFIG['INTERVAL']}\n"
        f"✅ Aktiv"
    )

    client = await AsyncClient.create()
    tasks = [asyncio.create_task(worker(client, s)) for s in CONFIG['SYMBOLS']]
    tasks.append(asyncio.create_task(daily_report()))
    tasks.append(asyncio.create_task(health_check()))
    tasks.append(asyncio.create_task(daily_diagnostics()))
    if CONFIG['FUND_BOT']:
        tasks.append(asyncio.create_task(news_worker()))

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