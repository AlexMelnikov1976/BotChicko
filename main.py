import os
import threading
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from forecast import forecast, forecast_for_period
from utils import (
    read_data,
    send_to_telegram,
    format_ruble,
    get_management_foodcost,
    get_management_percent,
    get_management_value,
)

# === Настройки ===
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def analyze(df):
    last_date = df["Дата"].max()
    if pd.isna(last_date):
        return "📅 Дата: не определена\n\n⚠️ Нет доступных данных"

    today_df = df[df["Дата"] == last_date]
    bar = round(today_df["Выручка бар"].sum())
    kitchen = round(today_df["Выручка кухня"].sum())
    total = bar + kitchen
    avg_check = round(today_df["Ср. чек общий"].mean())
    depth = round(today_df["Ср. поз чек общий"].mean() / 10, 1)
    hall_income = round(today_df["Зал начислено"].sum())
    delivery = round(today_df["Выручка доставка "].sum())
    hall_share = (hall_income / total * 100) if total else 0
    delivery_share = (delivery / total * 100) if total else 0

    foodcost_raw = today_df["Фудкост общий, %"].astype(str)\
        .str.replace(",", ".")\
        .str.replace("%", "")\
        .str.strip()
    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean() / 10, 1)

    discount_raw = today_df["Скидка общий, %"].astype(str)\
        .str.replace(",", ".")\
        .str.replace("%", "")\
        .str.strip()
    discount = round(pd.to_numeric(discount_raw, errors="coerce").mean() / 10, 1)

    avg_check_emoji = "🙂" if avg_check >= 1300 else "🙁"
    foodcost_emoji = "🙂" if foodcost <= 23 else "🙁"

    managers_today = today_df["Менеджер"].dropna().unique()
    manager_name = managers_today[0] if len(managers_today) > 0 else "—"

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"👤 {manager_name}\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Ср.чек: {format_ruble(avg_check)} {avg_check_emoji}\n"
        f"📏 Глубина: {depth:.1f}\n"
        f"🪑 ЗП зал: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)} ({delivery_share:.1f}%)\n"
        f"📊 Доля ЗП зала: {hall_share:.1f}%\n"
        f"🍔 Фудкост: {foodcost}% {foodcost_emoji}\n"
        f"💸 Скидка: {discount}%"
    )

# --- Обработка команд ---

async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        result = forecast(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=result)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def forecast_period_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        # Аргумент: current/previous/last
        period = 'current'
        if context.args:
            arg = context.args[0].lower()
            if arg in ('previous', 'last', 'prev'):
                period = 'previous'
        result = forecast_for_period(df, period)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=result)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        report = analyze(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=report)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        if "Менеджер" not in df.columns:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Колонка 'Менеджер' не найдена в данных.")
            return
        now = datetime.now()
        filtered = df[
            df["Менеджер"].notna() &
            (df["Дата"].dt.year == now.year) &
            (df["Дата"].dt.month == now.month)
        ]
        if filtered.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет строк с указанными менеджерами за текущий месяц.")
            return

        manager_stats = filtered.groupby("Менеджер").agg({
            "Выручка бар": "sum",
            "Выручка кухня": "sum",
            "Ср. чек общий": "mean",
            "Ср. поз чек общий": "mean",
            "Скидка общий, %": "mean"
        }).fillna(0)

        manager_stats["Общая выручка"] = manager_stats["Выручка бар"] + manager_stats["Выручка кухня"]
        manager_stats["Глубина"] = manager_stats["Ср. поз чек общий"] / 10

        max_values = {
            "Ср. чек общий": manager_stats["Ср. чек общий"].max(),
            "Общая выручка": manager_stats["Общая выручка"].max(),
            "Глубина": manager_stats["Глубина"].max()
        }

        manager_stats["Оценка"] = (
            (manager_stats["Ср. чек общий"] / max_values["Ср. чек общий"]) * 0.5 +
            (manager_stats["Общая выручка"] / max_values["Общая выручка"]) * 0.3 +
            (manager_stats["Глубина"] / max_values["Глубина"]) * 0.2
        )

        manager_stats = manager_stats.sort_values("Оценка", ascending=False)
        message = f"📅 Период: {now.strftime('%B %Y')}\n\n"
        for name, row in manager_stats.iterrows():
            discount_percent = round(row['Скидка общий, %'] / 10, 1)
            message += (
                f"👤 {name}\n"
                f"📊 Выручка: {format_ruble(row['Общая выручка'])}\n"
                f"🧾 Ср. чек: {format_ruble(row['Ср. чек общий'])}\n"
                f"📏 Глубина: {row['Глубина']:.1f}\n"
                f"💸 Скидка: {discount_percent}%\n\n"
            )
        message += f"🏆 Победитель: {manager_stats.index[0]}"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# --- Планировщик для ежедневного отчёта ---
def job():
    try:
        df = read_data()
        report = analyze(df)
        send_to_telegram(report)
    except Exception as e:
        send_to_telegram(f"❌ Ошибка: {str(e)}")

def get_month_period(period='current'):
    today = datetime.now()
    if period == 'current':
        start_date = today.replace(day=1)
        end_date = today
    elif period == 'previous':
        first_day_this_month = today.replace(day=1)
        last_day_prev_month = first_day_this_month - timedelta(days=1)
        start_date = last_day_prev_month.replace(day=1)
        end_date = last_day_prev_month
    else:
        raise ValueError("period должен быть 'current' или 'previous'")
    return start_date, end_date

if __name__ == "__main__":
    print("⏰ Тестовый запуск без Telegram\n")
    df = read_data()
    print("=== Анализ дня ===")
    print(analyze(df))
    print("=== Прогноз ===")
    print(forecast(df))
    print("=== Прогноз за прошлый месяц ===")
    print(forecast_for_period(df, period='previous'))
    print("⏰ Бот запущен. Отчёт будет в 9:30 по Калининграду")
    send_to_telegram("⚡️ Перезапуск")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("forecast", forecast_command))
    app.add_handler(CommandHandler("managers", managers_command))
    app.add_handler(CommandHandler("forecast_period", forecast_period_command))  # Новый хендлер

    scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")
    scheduler.add_job(job, trigger="cron", hour=9, minute=30)
    threading.Thread(target=scheduler.start).start()
    app.run_polling()
