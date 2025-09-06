# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- Настройки подключения к базе данных ---
DB_CONFIG = {
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'database': os.getenv('POSTGRES_DB'),
    'host': 'localhost'
}

# --- Настройки парсера ---
CONCURRENCY_LIMIT = 5
DELAY_BETWEEN_PAGES = (1.0, 2.5)
DELAY_BETWEEN_CATEGORIES = (2.0, 4.0)

# --- Директория для сохранения JSON-файлов с URL-адресами ---
URLS_DIR = 'parsed_urls' # <--- ДОБАВЬТЕ ЭТУ СТРОКУ

# --- Настройки для сохранения изображений (если понадобится в будущем) ---
IMAGES_DIR = 'static/images/products'
