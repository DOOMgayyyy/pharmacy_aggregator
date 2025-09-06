# # parsers/base_parser.py
# import asyncio
# import re
# from abc import ABC, abstractmethod
# import asyncpg
# import httpx

# def light_normalize(name: str) -> str:
#     """
#     Легкая нормализация: приводит к нижнему регистру,
#     убирает пунктуацию и лишние пробелы, но СОХРАНЯЕТ ЦИФРЫ.
#     """
#     name = name.lower()
#     # Заменяем дефисы, запятые и слэши на пробелы
#     name = re.sub(r'[-,\/]', ' ', name)
#     # Убираем все символы, кроме букв, цифр и пробелов
#     name = re.sub(r'[^a-zа-я0-9\s]', '', name)
#     # Схлопываем множественные пробелы в один
#     return ' '.join(name.split())
    
# class BaseParser(ABC):
#     """Абстрактный 'скелет' для всех парсеров."""
#     def __init__(self, session: httpx.AsyncClient, db_pool: asyncpg.Pool):
#         self.session = session
#         self.db_pool = db_pool

#     async def fetch_html(self, url: str) -> str | None:
#         try:
#             await asyncio.sleep(0.5)
#             response = await self.session.get(url, timeout=20, follow_redirects=True)
#             response.raise_for_status()
#             return response.text
#         except httpx.RequestError as e:
#             print(f"🚫 Ошибка загрузки {url}: {e}")
#             return None

#     @abstractmethod
#     async def get_pharmacy_id(self) -> int:
#         """Каждый парсер должен уметь получать ID своей аптеки в БД."""
#         pass
import asyncio
import re
from abc import ABC
import asyncpg
import httpx
from datetime import datetime
import random
import json
import aiofiles # For async file operations

# light_normalize function remains the same...
def light_normalize(name: str) -> str:
    # ...
    name = name.lower()
    name = re.sub(r'[-,\/]', ' ', name)
    name = re.sub(r'[^a-zа-я0-9\s]', '', name)
    return ' '.join(name.split())

class BaseParser(ABC):
    """Abstract base class for all parsers."""
    def __init__(self, session: httpx.AsyncClient, db_pool: asyncpg.Pool = None):
        self.base_url = 'https://gosapteka18.ru'
        self.session = session
        self.db_pool = db_pool
        # Lock to prevent race conditions when writing to the log file
        self.log_lock = asyncio.Lock()

    async def fetch_html(self, url: str, timeout: int = 20) -> str | None:
        try:
            # Increased delay slightly for more stability
            await asyncio.sleep(random.uniform(0.5, 1.5))
            response = await self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            # The print statement remains for real-time feedback
            print(f"🚫 Download error {url}: {str(e)}")
            return None
        except httpx.HTTPStatusError as e:
            print(f"🚫 Status error {e.response.status_code} for {url}: {str(e)}")
            return None

    async def log_error(self, url: str, breadcrumbs: list[str], error: str):
        """Asynchronously logs a failed URL and its context to a JSON file."""
        log_filename = f"log_error_{datetime.now().strftime('%Y-%m-%d')}.json"
        new_entry = {"url": url, "breadcrumbs": breadcrumbs, "error": error}

        async with self.log_lock: # Ensure only one task writes to the file at a time
            try:
                # Try to read existing data
                async with aiofiles.open(log_filename, mode='r', encoding='utf-8') as f:
                    content = await f.read()
                    data = json.loads(content)
            except (FileNotFoundError, json.JSONDecodeError):
                # If file doesn't exist or is empty/corrupt, start with an empty list
                data = []

            # Append new error and write back
            data.append(new_entry)
            async with aiofiles.open(log_filename, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))