import os                         # Для работы с переменными окружения
from dotenv import load_dotenv    # Для загрузки .env файла

load_dotenv()                     # Загрузка всех переменных окружения из файла .env

SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"  # ID основной Google-таблицы с операционными данными
MANAGEMENT_SHEET_ID = "1nqpQ97D9rS2hPVQrrlbPKO5QG5RXvc936xvw6TSHnXc"  # ID управляющей Google-таблицы
MANAGEMENT_SHEET_NAME = "Лист1"    # Имя листа в управляющей таблице

SERVICE_ACCOUNT_FILE = 'fifth-medley-461515-h0-089884c74c28.json'  # JSON строка с ключом сервисного аккаунта
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]  # Права доступа только на чтение

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')  # если хотите хранить токен в .env
CHAT_ID = os.getenv('CHAT_ID')                # если хотите хранить chat_id в .env

##import os                         # Для работы с переменными окружения
##from dotenv import load_dotenv    # Для загрузки .env файла

##load_dotenv()                     # Загрузка всех переменных окружения из файла .env

##SHEET_ID = "1SHHKKcgXgbzs_AyBQJpyHx9zDauVz6iR9lz1V7Q3hyw"  # ID основной Google-таблицы с операционными данными
##MANAGEMENT_SHEET_ID = "1nqpQ97D9rS2hPVQrrlbPKO5QG5RXvc936xvw6TSHnXc"  # ID управляющей Google-таблицы
##MANAGEMENT_SHEET_NAME = "Лист1"    # Имя листа в управляющей таблице

##GOOGLE_CREDENTIALS = os.environ['GOOGLE_CREDENTIALS']  # JSON строка с ключом сервисного аккаунта
##SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]  # Права доступа только на чтение

