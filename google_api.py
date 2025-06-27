import json                                     # Для обработки JSON-ключа сервисного аккаунта
import pandas as pd                             # Для работы с таблицами данных
import gspread                                  # Для подключения к Google Sheets через API
from google.oauth2 import service_account        # Для авторизации Google API
from config import *                            # Импортируем все переменные из config.py

# --- ВАЖНО: Используем путь к json-файлу! ---
SERVICE_ACCOUNT_FILE = "fifth-medley-461515-h0-089884c74c28.json"

def get_creds():
    """Создание объекта авторизации для Google API."""
    # --- Если используем файл ---
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

    # --- Если вдруг захотите использовать строку из env (.env или config.py) ---
    # return service_account.Credentials.from_service_account_info(
    #     json.loads(GOOGLE_CREDENTIALS),         # Преобразуем JSON-строку из .env в dict
    #     scopes=SCOPES                           # Устанавливаем права только на чтение Google Sheets
    # )

def read_main_data():
    """Чтение основной таблицы (операционной) и возврат pandas.DataFrame."""
    gc = gspread.authorize(get_creds())                      # Авторизация сервисным аккаунтом
    sheet = gc.open_by_key(SHEET_ID).sheet1                  # Открываем первый лист основной таблицы
    df = pd.DataFrame(sheet.get_all_records())               # Загружаем все строки в DataFrame

    # Преобразование типов данных: числовые столбцы и дата
    if "Дата" in df.columns:
        for col in df.columns:
            if col not in ["Дата", "Фудкост общий, %", "Менеджер"]:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(",", ".")                   # Меняем запятые на точки
                    .str.replace(r"[^\d\.]", "", regex=True) # Убираем всё кроме цифр и точки
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")  # Преобразуем к числам

        df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")  # Дату приводим к datetime
        df = df.dropna(subset=["Дата"])                    # Удаляем строки без даты

    return df

def get_management_percent(row_name: str):
    """
    Возвращает числовое значение из управляющей таблицы по названию строки (столбец 'Процент'), 
    поддерживает все форматы записи: 3,2 / 3.2 / 3,2% / 3.2% / 3% / '3' и пр.
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    found = df[df.iloc[:, 0].astype(str).str.lower().str.strip() == row_name.lower().strip()]
    if not found.empty and "Процент" in df.columns:
        value = found.iloc[0]["Процент"]
        # Точно как для фудкоста: приведение к строке, замена , на ., удаление %, strip, через pd.to_numeric
        value_str = str(value).replace("%", "").replace(",", ".").strip()
        # Иногда value может быть float (например, 3.2) - в этом случае преобразование корректно
        value_num = pd.to_numeric(value_str, errors="coerce")
        return value_num
    return None

def get_management_value(row_name: str, column_name: str):
    """
    Возвращает значение по названию строки и столбца из управляющей таблицы.
    Например, row_name='ЗП упр', column_name='Сумма'
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    found = df[df.iloc[:,0].astype(str).str.lower().str.strip() == row_name.lower().strip()]
    if not found.empty and column_name in df.columns:
        value = found.iloc[0][column_name]
        try:
            return float(str(value).replace(",", "."))
        except Exception:
            return None
    return None

def get_management_bonus_grid(manager_name):
    """
    Возвращает DataFrame с бонусной сеткой из управляющей таблицы по роли.
    """
    gc = gspread.authorize(get_creds())
    sheet = gc.open_by_key(MANAGEMENT_SHEET_ID).worksheet(MANAGEMENT_SHEET_NAME)
    df = pd.DataFrame(sheet.get_all_records())
    # Оставляем только строки, где в первом столбце совпадает manager_name
    return df[df.iloc[:,0].astype(str).str.lower().str.contains(manager_name.lower())][["Минимум", "Максимум", "Бонус"]].reset_index(drop=True)

# --- СТАРЫЕ ВАРИАНТЫ оставлены только для справки, НИЖЕ НЕ НУЖНЫ ---
# (Весь старый код ниже строки оставить закомментированным, как показано выше.)
