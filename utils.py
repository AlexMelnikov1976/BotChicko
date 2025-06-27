# utils.py

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2 import service_account
from config import (
    SHEET_ID,
    MANAGEMENT_SHEET_ID,
    MANAGEMENT_SHEET_NAME,
    SCOPES,
    SERVICE_ACCOUNT_FILE
)

load_dotenv()  # обязательно загрузить переменные из .env, если не сделали ранее

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Авторизация через JSON-файл сервисного аккаунта
def get_creds():
    """Создание объекта авторизации для Google API."""
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

def format_ruble(val, decimals=0):
    """Красивое оформление суммы в рублях с пробелами."""
    if pd.isna(val):
        return "—"
    formatted = f"{val:,.{decimals}f}₽".replace(",", " ")
    if decimals == 0:
        formatted = formatted.replace(".00", "")
    return formatted

def read_data():
    """Чтение основной таблицы (операционной) и возврат pandas.DataFrame."""
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(SHEET_ID).sheet1
    df = pd.DataFrame(sheet.get_all_records())

    if "Дата" in df.columns:
        for col in df.columns:
            if col not in ["Дата", "Фудкост общий, %", "Менеджер"]:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(",", ".")
                    .str.replace(r"[^\d\.]", "", regex=True)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["Дата"])
    return df

def get_management_percent(row_name: str):
    """
    Возвращает число из управляющей таблицы по названию строки (столбец 'Процент').
    Поддерживает любые форматы (3.2, 3,2%, '3', '3.2%' и т.д.)
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    found = df[df.iloc[:, 0].astype(str).str.lower().str.strip() == row_name.lower().strip()]
    if not found.empty and "Процент" in df.columns:
        value = found.iloc[0]["Процент"]
        value_str = str(value).replace("%", "").replace(",", ".").strip()
        value_num = pd.to_numeric(value_str, errors="coerce")
        return value_num
    return None

def get_management_value(row_name: str, column_name: str):
    """
    Возвращает значение по названию строки и столбца из управляющей таблицы.
    Пример: row_name='ЗП упр', column_name='Сумма'
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    found = df[df.iloc[:, 0].astype(str).str.lower().str.strip() == row_name.lower().strip()]
    if not found.empty and column_name in df.columns:
        value = found.iloc[0][column_name]
        try:
            return float(str(value).replace(",", "."))
        except Exception:
            return None
    return None

def get_management_foodcost():
    gc = gspread.authorize(CREDS)
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    fc_row = df[df.iloc[:,0].astype(str).str.lower().str.strip() == "фудкост"]
    if not fc_row.empty:
        fc_percent = fc_row.iloc[0]["Процент"]
        try:
            return float(str(fc_percent).replace(",", "."))
        except Exception:
            return None
    return None

def send_to_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, data=data)

def get_management_bonus_grid(manager_name):
    """
    Возвращает DataFrame с бонусной сеткой из управляющей таблицы по роли.
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    # Возвращаем только подходящие строки
    return df[df.iloc[:, 0].astype(str).str.lower().str.contains(manager_name.lower())][["Минимум", "Максимум", "Бонус"]].reset_index(drop=True)
