# ============================================================
# EngulfingTrend Bot v5.9.4 — DUAL ENTRY + BODY ENGULFING + CONFIRMATION
# Procfile / Config Vars orqali ishga tushirish uchun moslashtirilgan.
#
# ESLATMA: bu — SIGNAL/SIMULYATSIYA boti. Haqiqiy order qo'ymaydi,
# faqat Binance narx oqimini o'qib, xayoliy balansda hisoblab, Telegramga yozadi.
# ============================================================
import os
import asyncio
import logging
import time
import io
from datetime import datetime, timezone
from binance import AsyncClient, BinanceSocketManager
from telegram import Bot, InputFile
from telegram.constants import ParseMode
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

# ------------------------------------------------------------
# Ikkita nusxani ishga tushirsangiz (masalan ikkita alohida app/dyno),
# har birining "Config Vars" (Environment Variables) bo'limida
# boshqa BOT_NAME, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, SYMBOLS bering.
# Kod bir xil, sozlamalar platforma orqali beriladi — .env yoki
# config.json shart emas.
# ------------------------------------------------------------

def env(key, default=None):
    return os.environ.get(key, default)
