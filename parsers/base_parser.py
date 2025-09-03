# parsers/base_parser.py
import asyncio
import re
from abc import ABC, abstractmethod
import asyncpg
import httpx

def normalize_name(name: str) -> str:
    """Главная функция очистки названий для точного сопоставления."""
    name = name.lower()
    patterns_to_remove = [
        r'\d+\s*(мг|мл|шт|доз|г|см|мкг)', r'№\d+',
        r'(таб|капс|р-р|мазь|гель|крем|пор|супп|амп|спрей)',
        r'(п/о|п/п/о|шип|жеват|дисперг)', r'(\(.*\)|\[.*\])'
    ]
    for pattern in patterns_to_remove:
        name = re.sub(pattern, ' ', name, flags=re.IGNORECASE)
    return ' '.join(name.split())

class BaseParser(ABC):
    """Абстрактный 'скелет' для всех парсеров."""
    def __init__(self, session: httpx.AsyncClient, db_pool: asyncpg.Pool):
        self.session = session
        self.db_pool = db_pool

    async def fetch_html(self, url: str) -> str | None:
        try:
            await asyncio.sleep(0.5)
            response = await self.session.get(url, timeout=20, follow_redirects=True)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            print(f"🚫 Ошибка загрузки {url}: {e}")
            return None

    @abstractmethod
    async def get_pharmacy_id(self) -> int:
        """Каждый парсер должен уметь получать ID своей аптеки в БД."""
        pass