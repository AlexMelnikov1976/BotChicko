import os  # Для работы с переменными окружения
import json  # Для загрузки JSON-ключей Google
import calendar  # Для определения количества дней в месяце
import threading  # Для параллельного запуска задач
import pandas as pd  # Для анализа и обработки таблиц
import matplotlib.pyplot as plt  # Импортирован, но не используется
import gspread  # Для подключения к Google Sheets
import requests  # Для отправки HTTP-запросов (Telegram API)
from datetime import datetime, timedelta  # Работа с датами и временем
from dotenv import load_dotenv  # Для загрузки .env файла
from google.oauth2 import service_account  # Авторизация в Google API
from apscheduler.schedulers.blocking import BlockingScheduler  # Планировщик задач
from telegram import Update  # Telegram update object
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes  # Telegram bot API

# === Настройки ===
load_dotenv()  # Загружаем переменные из .env
SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"  # ID Google-таблицы
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # Telegram токен из .env
CHAT_ID = os.getenv("CHAT_ID")  # ID чата для отправки отчётов
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]  # Только чтение

# Авторизация в Google API
CREDS = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['GOOGLE_CREDENTIALS']),
    scopes=SCOPES
)

# Форматирует число в формат рубля
def format_ruble(val, decimals=0):
    if pd.isna(val):
        return "—"
    formatted = f"{val:,.{decimals}f}₽".replace(",", " ")
    if decimals == 0:
        formatted = formatted.replace(".00", "")
    return formatted

# Отправляет текстовое сообщение в Telegram
def send_to_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=data)

# Загружает и обрабатывает данные из Google Sheets
def read_data():
    print("Читаем таблицу...")
    gc = gspread.authorize(CREDS)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    df = pd.DataFrame(sheet.get_all_records())
    print("Столбцы из Google Sheets:", list(df.columns))

    if "Дата" not in df.columns:
        return pd.DataFrame()

    # Приведение числовых данных к корректному формату
    for col in df.columns:
        if col not in ["Дата", "Фудкост общий, %"]:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".")
                .str.replace(r"[^\d\.]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Преобразуем дату в datetime и удаляем строки без даты
    df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Дата"])
    print("Уникальные даты после парсинга:", df["Дата"].unique())
    print(f"Успешно прочитали! {df.shape}")
    return df

# Генерирует текст отчёта по последней дате
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

    foodcost_raw = today_df["Фудкост общий, %"].astype(str).str.replace(",", ".").str.replace("%", "").str.strip()
    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean() / 100, 1)

    avg_check_emoji = "🙂" if avg_check >= 1300 else "🙁"
    foodcost_emoji = "🙂" if foodcost <= 23 else "🙁"

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Ср.чек: {format_ruble(avg_check)} {avg_check_emoji}\n"
        f"📏 Глубина: {depth:.1f}\n"
        f"🪑 ЗП зал: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)} ({delivery_share:.1f}%)\n"
        f"📊 Доля ЗП зала: {hall_share:.1f}%\n"
        f"🍔 Фудкост: {foodcost}% {foodcost_emoji}"
    )

# Команда /analyze в Telegram
async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        report = analyze(df)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=report)
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# Команда /forecast в Telegram
async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        df = read_data()
        now = datetime.now()
        current_month_df = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]

        if current_month_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет данных за текущий месяц.")
            return

        total_revenue_series = current_month_df["Выручка бар"] + current_month_df["Выручка кухня"]
        salary_series = current_month_df["Начислено"]

        avg_daily_revenue = total_revenue_series.mean()
        avg_daily_salary = salary_series.mean()

        days_in_month = calendar.monthrange(now.year, now.month)[1]

        forecast_revenue = avg_daily_revenue * days_in_month
        fixed_salaries = 600_000
        forecast_salary = avg_daily_salary * days_in_month + fixed_salaries
        labor_cost_share = (forecast_salary / forecast_revenue * 100) if forecast_revenue else 0

        message = (
            f"📅 Прогноз на {now.strftime('%B %Y')}:\n"
            f"📊 Выручка: {format_ruble(forecast_revenue)}\n"
            f"🪑 ЗП: {format_ruble(forecast_salary)} (LC: {labor_cost_share:.1f}%)"
        )
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# Команда /managers в Telegram
async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_chat.id) != str(CHAT_ID):
        return
    try:
        print("📥 /managers команду получили!")
        await context.bot.send_message(chat_id=update.effective_chat.id, text="📥 Команда получена!")

        df = read_data()
        now = datetime.now()
        current_month_df = df[(df["Дата"].dt.year == now.year) & (df["Дата"].dt.month == now.month)]

        if current_month_df.empty or "Менеджер" not in current_month_df.columns:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет данных о менеджерах.")
            return

        # Группируем по менеджерам и считаем нужные метрики
        manager_stats = current_month_df.dropna(subset=["Менеджер"]).groupby("Менеджер").agg({
            "Выручка бар": "sum",
            "Выручка кухня": "sum",
            "Ср. чек общий": "mean",
            "Ср. поз чек общий": "mean"
        }).fillna(0)

        manager_stats["Общая выручка"] = manager_stats["Выручка бар"] + manager_stats["Выручка кухня"]
        top_manager = manager_stats.sort_values("Общая выручка", ascending=False).head(1)

        # Защита от ложных пустых значений
        if top_manager.shape[0] == 0 or top_manager.index.size == 0:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Не удалось определить лучшего менеджера.")
            return

        name = str(top_manager.index[0])
        total = top_manager["Общая выручка"].values[0]
        avg_check = top_manager["Ср. чек общий"].values[0]
        avg_depth = top_manager["Ср. поз чек общий"].values[0] / 10

        print("TOP MANAGER:", name, total, avg_check, avg_depth)  # Отладочный вывод в консоль

        message = (
            f"🏆 Лучший менеджер за {now.strftime('%B %Y')}:\n\n"
            f"👤 {name}\n"
            f"📊 Выручка: {format_ruble(total)}\n"
            f"🧾 Ср. чек: {format_ruble(avg_check)}\n"
            f"📏 Глубина чека: {avg_depth:.1f}"
        )

        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# Планировщик отправки ежедневного отчёта
def job():
    try:
        df = read_data()
        report = analyze(df)
        send_to_telegram(report)
    except Exception as e:
        send_to_telegram(f"❌ Ошибка: {str(e)}")

# Основной блок запуска бота
if __name__ == "__main__":
    print("⏰ Бот запущен. Отчёт будет в 9:30 по Калининграду")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Регистрация команд бота
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("forecast", forecast_command))
    app.add_handler(CommandHandler("managers", managers_command))

    # Ежедневный отчёт через планировщик
    scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")
    scheduler.add_job(job, trigger="cron", hour=9, minute=30)
    threading.Thread(target=scheduler.start).start()

    # Запуск polling — прослушивание команд
    app.run_polling()
