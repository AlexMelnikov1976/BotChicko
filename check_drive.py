from google.oauth2 import service_account
from googleapiclient.discovery import build

# === Настройки ===
SERVICE_ACCOUNT_FILE = 'credentials.json'  # имя JSON-файла с ключом
FOLDER_ID = 'PASTE_YOUR_FOLDER_ID_HERE'  # замени на ID папки из Google Диска

# === Подключение ===
def get_drive_service():
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

# === Получить список файлов ===
def list_files(folder_id):
    service = get_drive_service()
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        pageSize=10,
        fields="files(id, name, mimeType)"
    ).execute()
    files = results.get('files', [])
    if not files:
        print("📁 В папке ничего нет.")
    else:
        print("📄 Найденные файлы:")
        for file in files:
            print(f"- {file['name']} ({file['mimeType']})")

# === Запуск ===
if __name__ == "__main__":
    list_files("1_X2dNcyeOw9uMdFighkPOedCURZZu9QC")
