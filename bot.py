#!/usr/bin/env python3
"""ربات ژورنال معاملاتی حرفه‌ای (تلگرام) - تک‌کاربره - اجرا روی GitHub Actions
دیتابیس داخل خود تلگرام ذخیره می‌شود (فایل پین‌شده)؛ نیازی به سرور نیست.
env: BOT_TOKEN, OWNER_ID, [STORE_CHAT_ID], [RUN_SECONDS], [PROXY], [DB_PATH]
"""
import asyncio
import hashlib
import io
import logging
import os
import signal
import sqlite3
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, ApplicationHandlerStop, CallbackQueryHandler,
                          CommandHandler, ContextTypes, ConversationHandler,
                          MessageHandler, TypeHandler, filters)

TOKEN = os.environ["BOT_TOKEN"]
OWNER = int(os.environ["OWNER_ID"])
