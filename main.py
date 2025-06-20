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
load_dotenv()  # Загружаем переменные из .env файла
SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"  # ID Google-таблицы
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # Telegram токен
CHAT_ID = os.getenv("CHAT_ID")  # Telegram chat ID
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]  # Права только на чтение

# Авторизация в Google API
CREDS = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['GOOGLE_CREDENTIALS']),
    scopes=SCOPES
)

# Форматирует число как рубли
def format_ruble(val, decimals=0):
    if pd.isna(val):
        return "—"
    formatted = f"{val:,.{decimals}f}₽".replace(",", " ")
    if decimals == 0:
        formatted = formatted.replace(".00", "")
    return formatted

# Отправка сообщения в Telegram
def send_to_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=data)

# Загрузка и подготовка данных из Google Sheets
def read_data():
    print("Читаем таблицу...")
    gc = gspread.authorize(CREDS)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    df = pd.DataFrame(sheet.get_all_records())
    print("Столбцы из Google Sheets:", list(df.columns))

    if "Дата" not in df.columns:
        return pd.DataFrame()

    for col in df.columns:
        if col not in ["Дата", "Фудкост общий, %", "Менеджер", "Скидка общий, %"]:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", ".")
                .str.replace(r"[^\d\.]", "", regex=True)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Дата"])
    print("Уникальные даты после парсинга:", df["Дата"].unique())
    print(f"Успешно прочитали! {df.shape}")
    return df

# Анализ показателей за последний день
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
    foodcost = round(pd.to_numeric(foodcost_raw, errors="coerce").mean(), 1)

    avg_check_emoji = "🙂" if avg_check >= 1300 else "🙁"
    foodcost_emoji = "🙂" if foodcost <= 23 else "🙁"

    # Определяем уникальных менеджеров за этот день
    managers_today = today_df["Менеджер"].dropna().unique()
    managers_str = ", ".join(managers_today) if len(managers_today) > 0 else "не указано"

    return (
        f"📅 Дата: {last_date.strftime('%Y-%m-%d')}\n\n"
        f"👤 Менеджер(ы): {managers_str}"\n"
        f"📊 Выручка: {format_ruble(total)} (Бар: {format_ruble(bar)} + Кухня: {format_ruble(kitchen)})\n"
        f"🧾 Ср.чек: {format_ruble(avg_check)} {avg_check_emoji}\n"
        f"📏 Глубина: {depth:.1f}\n"
        f"🪑 ЗП зал: {format_ruble(hall_income)}\n"
        f"📦 Доставка: {format_ruble(delivery)} ({delivery_share:.1f}%)\n"
        f"📊 Доля ЗП зала: {hall_share:.1f}%\n"
        f"🍔 Фудкост: {foodcost}% {foodcost_emoji}"
        
    )

# Команда /managers: сравнение менеджеров по ключевым метрикам
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

        # Очистка и фильтрация скидки
        filtered["Скидка общий, %"] = filtered["Скидка общий, %"].astype(str).str.replace(",", ".").str.replace("%", "").str.strip()
        filtered["Скидка общий, %"] = pd.to_numeric(filtered["Скидка общий, %"], errors="coerce")
        filtered = filtered[filtered["Скидка общий, %"] < 100]

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
            "Глубина": manager_stats["Глубина"].max(),
            "Скидка общий, %": manager_stats["Скидка общий, %"].max()
        }

        manager_stats["Оценка"] = (
            (manager_stats["Ср. чек общий"] / max_values["Ср. чек общий"]) * 0.5 +
            (manager_stats["Глубина"] / max_values["Глубина"]) * 0.2 +
            (manager_stats["Общая выручка"] / max_values["Общая выручка"]) * 0.2 -
            (manager_stats["Скидка общий, %"] / max_values["Скидка общий, %"]) * 0.1
        )

        manager_stats = manager_stats.sort_values("Оценка", ascending=False)
        message = f"📅 Период: {now.strftime('%B %Y')}\n\n"
        for name, row in manager_stats.iterrows():
            message += (
                f"👤 {name}\n"
                f"📊 Выручка: {format_ruble(row['Общая выручка'])}\n"
                f"🧾 Ср. чек: {format_ruble(row['Ср. чек общий'])}\n"
                f"📏 Глубина: {row['Глубина']:.1f}\n"
                f"💸 Скидка: {round(row['Скидка общий, %'], 1)}%\n\n"
            )

        message += f"🏆 Победитель: {manager_stats.index[0]}"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message)

    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Ошибка: {str(e)}")

# Планировщики отчётов
scheduler = BlockingScheduler(timezone="Europe/Kaliningrad")

# Ежедневный отчёт по последнему дню
scheduler.add_job(lambda: send_to_telegram(analyze(read_data())), trigger="cron", hour=9, minute=30)

# Еженедельный отчёт по менеджерам (понедельник)
scheduler.add_job(lambda: asyncio.run(managers_command(Update(update_id=0, message=None), ContextTypes.DEFAULT_TYPE())), trigger="cron", day_of_week="mon", hour=9, minute=30)

# Точка входа — запуск бота
if __name__ == "__main__":
    print("⏰ Бот запущен. Отчёты: ежедневно и по понедельникам в 9:30")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("managers", managers_command))

    threading.Thread(target=scheduler.start).start()
    app.run_polling()
