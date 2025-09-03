# parsers/gosapteka/details_processor_task.py
import asyncio
import os
import re
import json
from urllib.parse import urljoin

import asyncpg
import httpx
from bs4 import BeautifulSoup, Tag

# ИЗМЕНЕНО: Импортируем нашу функцию для нормализации имен
from ..base_parser import normalize_name
from config import DB_CONFIG, URLS_DIR, CONCURRENCY_LIMIT

class ProductProcessor:
    def __init__(self, session: httpx.AsyncClient, db_pool: asyncpg.Pool):
        self.base_url = 'https://gosapteka18.ru'
        self.session = session
        self.db_pool = db_pool
        self.price_regexes = [
            re.compile(r'product-card__price-value[^>]*>([\d\s,]+)'),
            re.compile(r'"price"\s*:\s*"(\d+\.?\d*)"'),
            re.compile(r'itemprop="price"[^>]+content="(\d+\.?\d*)"')
        ]
        self.pharmacy_name = "Госаптека 18"

    async def fetch_html(self, url: str) -> str | None:
        try:
            await asyncio.sleep(0.5)
            response = await self.session.get(url, timeout=20)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"🚫 Ошибка загрузки {url}: {e}")
            return None

    def _get_title(self, soup: BeautifulSoup) -> str:
        # Ваш селектор может быть более точным, используем его
        tag = soup.select_one('h1.title.headline-main__title.product-card__title, h1.product-card__title')
        return tag.get_text(strip=True) if tag else "Без названия"

    def _get_image(self, soup: BeautifulSoup) -> str:
        tag = soup.select_one('img.product-card__picture-view-img')
        return urljoin(self.base_url, tag['src']) if tag and 'src' in tag.attrs else ""

    def _get_description(self, soup: BeautifulSoup) -> str:
        block = soup.select_one('div.product-card__description')
        if not block: return "Нет данных"
        # Ваша логика парсинга описания очень хорошая, оставляем ее
        sections = {h.get_text(strip=True): " ".join([s.get_text(" ", strip=True) for s in h.find_next_siblings() if s.name != 'h4' and isinstance(s, Tag)]) for h in block.find_all('h4')}
        return "\n\n".join([f"{k}:\n{v}" for k, v in sections.items()]) if sections else (block.get_text(" ", strip=True) or "Нет данных")

    def _get_price(self, html: str) -> float | None:
        for regex in self.price_regexes:
            if match := regex.search(html):
                price_str = match.group(1).replace(' ', '').replace(',', '.')
                return float(price_str)
        return None

    async def save_data(self, data: dict):
        async with self.db_pool.acquire() as conn, conn.transaction():
            # Шаг 1: Категории (без изменений)
            await conn.execute("INSERT INTO medicine_types (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", data['type_name'])
            type_id = await conn.fetchval("SELECT id FROM medicine_types WHERE name = $1", data['type_name'])

            # ИЗМЕНЕНО: Шаг 2: Вставляем `normalized_name` в таблицу medicines
            medicine_id = await conn.fetchval("""
                INSERT INTO medicines (name, normalized_name, description, image_url, type_id)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name) DO UPDATE SET
                    description = EXCLUDED.description,
                    image_url = EXCLUDED.image_url,
                    type_id = EXCLUDED.type_id,
                    normalized_name = EXCLUDED.normalized_name
                RETURNING id;
            """, data['name'], data['normalized_name'], data['description'], data['image_url'], type_id)

            # Шаг 3: Аптеки (без изменений)
            pharmacy_id = await conn.fetchval("SELECT id FROM pharmacies WHERE address = $1", self.base_url) or await conn.fetchval("INSERT INTO pharmacies (name, address) VALUES ($1, $2) RETURNING id", self.pharmacy_name, self.base_url)

            # ИЗМЕНЕНО: Шаг 4: Убираем `quantity` из запроса
            await conn.execute("""
                INSERT INTO pharmacy_prices (pharmacy_id, medicine_id, price)
                VALUES ($1, $2, $3)
                ON CONFLICT (pharmacy_id, medicine_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    last_updated = NOW();
            """, pharmacy_id, medicine_id, data['price'])


    async def process_product(self, product_url: str, category_name: str):
        print(f"⏳ Обработка: {product_url}")
        html = await self.fetch_html(product_url)
        if not html: return

        soup = BeautifulSoup(html, 'html.parser')
        title = self._get_title(soup)
        if title == "Без названия": return

        price = self._get_price(html)
        if price is None: return
        
        # ИЗМЕНЕНО: Генерируем `normalized_name` перед сохранением
        normalized = normalize_name(title)

        product_data = {
            'name': title,
            'normalized_name': normalized, # Добавляем его в словарь
            'description': self._get_description(soup),
            'image_url': self._get_image(soup),
            'price': price,
            'type_name': category_name or "Без категории"
        }

        await self.save_data(product_data)
        print(f"💾 Сохранено: {title} — {price} руб.")


async def process_details_from_files():
    """Главная функция этого модуля: читает файлы и запускает обработку."""
    if not os.path.exists(URLS_DIR) or not os.listdir(URLS_DIR):
        print(f"❌ Директория '{URLS_DIR}' пуста или не найдена. Запустите сначала сбор ссылок.")
        return

    db_pool = None
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG)
        print("✅ Подключение к базе данных установлено.")
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось подключиться к базе данных: {e}")
        return

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        processor = ProductProcessor(session, db_pool)
        tasks = []

        print(f"🔍 Чтение файлов из директории '{URLS_DIR}'...")
        for filename in os.listdir(URLS_DIR):
            if filename.endswith('.json'):
                filepath = os.path.join(URLS_DIR, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    category_name = data.get('category_name', 'Без категории')
                    for url in data['product_urls']:
                        # Обертка для корректной передачи переменных в асинхронную задачу
                        async def worker(p_url=url, cat_name=category_name):
                            async with semaphore:
                                await processor.process_product(p_url, cat_name)
                        tasks.append(asyncio.create_task(worker()))

        print(f"🚀 Запускается обработка {len(tasks)} товаров...")
        await asyncio.gather(*tasks)

    if db_pool:
        await db_pool.close()
        print("🔌 Соединение с базой данных закрыто.")

if __name__ == "__main__":
    asyncio.run(process_details_from_files())