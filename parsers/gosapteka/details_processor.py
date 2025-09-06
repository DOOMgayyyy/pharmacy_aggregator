# parsers/gosapteka/details_processor.py
import asyncio
import os
import re
import json
from urllib.parse import urljoin
import asyncpg
import httpx
from bs4 import BeautifulSoup, Tag
from ..base_parser import BaseParser
from config import DB_CONFIG, URLS_DIR, CONCURRENCY_LIMIT

class DetailsProcessor(BaseParser):
    """Parses product details and saves them to the database."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pharmacy_name = "Госаптека 18"
        # --- ИЗМЕНЕНИЕ 1: Жестко задаем base_url здесь ---
        self.base_url = "https://gosapteka18.ru"

    def _parse_product_data(self, soup: BeautifulSoup) -> dict:
        """Extracts all necessary data from a product page."""
        title_tag = soup.select_one('h1.title.headline-main__title.product-card__title, h1.product-card__title')
        title = title_tag.get_text(strip=True) if title_tag else "Без названия"
        
        img_tag = soup.select_one('img.product-card__picture-view-img')
        image_url = urljoin(self.base_url, img_tag['src']) if img_tag and 'src' in img_tag.attrs else ""
        
        desc_text = "Нет данных"
        desc_block = soup.select_one('div.product-card__description')
        if desc_block:
            sections = {h.get_text(strip=True): " ".join([s.get_text(" ", strip=True) for s in h.find_next_siblings() if s.name != 'h4' and isinstance(s, Tag)]) for h in desc_block.find_all('h4')}
            desc_text = "\n\n".join([f"{k}:\n{v}" for k, v in sections.items()]) or (desc_block.get_text(" ", strip=True) or desc_text)
        
        price = None
        meta_price_tag = soup.find('meta', {'itemprop': 'price'})
        if meta_price_tag and 'content' in meta_price_tag.attrs:
            try:
                price = float(meta_price_tag['content'])
            except (ValueError, TypeError):
                pass
        
        if price is None:
            price_match = re.search(r'"price"\s*:\s*"(\d+\.?\d*)"', soup.prettify())
            if price_match:
                price = float(price_match.group(1))
        
        return {'name': title, 'image_url': image_url, 'description': desc_text, 'price': price}

    async def _get_or_create_category_id(self, breadcrumbs: list[str], conn) -> int:
        """Finds or creates the full category path and returns the final category ID."""
        parent_id = None
        category_id = None
        for category_name in breadcrumbs:
            row = await conn.fetchrow("SELECT id FROM categories WHERE name = $1 AND (parent_id = $2 OR ($2 IS NULL AND parent_id IS NULL))", category_name, parent_id)
            if row:
                category_id = row['id']
            else:
                category_id = await conn.fetchval("INSERT INTO categories (name, parent_id) VALUES ($1, $2) RETURNING id", category_name, parent_id)
            parent_id = category_id
        return category_id

    async def process_item(self, product_url: str, breadcrumbs: list[str]):
        """Full processing cycle for one product URL."""
        print(f"⏳ Processing: {product_url}")
        html = await self.fetch_html(product_url)
        
        if not html:
            await self.log_error(product_url, breadcrumbs, "Failed to download HTML")
            return

        soup = BeautifulSoup(html, 'html.parser')
        data = self._parse_product_data(soup)
        
        if data['name'] == "Без названия" or data['price'] is None:
            print(f"   - Skipped: Missing title or price for {product_url}")
            return

        async with self.db_pool.acquire() as conn, conn.transaction():
            category_id = await self._get_or_create_category_id(breadcrumbs, conn)
            
            medicine_id = await conn.fetchval("""
                INSERT INTO medicines (name, description, image_url, category_id)
                VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO UPDATE
                SET description=EXCLUDED.description, image_url=EXCLUDED.image_url, category_id=EXCLUDED.category_id
                RETURNING id;
            """, data['name'], data['description'], data['image_url'], category_id)

            pharmacy_id = await conn.fetchval("SELECT id FROM pharmacies WHERE address = $1", self.base_url) or await conn.fetchval("INSERT INTO pharmacies (name, address) VALUES ($1, $2) RETURNING id", self.pharmacy_name, self.base_url)

            await conn.execute("""
                INSERT INTO pharmacy_prices (pharmacy_id, medicine_id, price) VALUES ($1, $2, $3)
                ON CONFLICT (pharmacy_id, medicine_id) DO UPDATE SET price = EXCLUDED.price, last_updated = NOW();
            """, pharmacy_id, medicine_id, data['price'])
        
        print(f"💾 Saved: {data['name']} - {data['price']} руб.")

async def process_details_from_files():
    """Main orchestrator function for processing details from saved files."""
    if not os.path.exists(URLS_DIR) or not os.listdir(URLS_DIR):
        print(f"❌ Directory '{URLS_DIR}' is empty or not found. Run stage1 first.")
        return

    db_pool = None
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Critical Error: Could not connect to the database: {e}")
        return

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        # --- ИЗМЕНЕНИЕ 2: Убираем аргумент base_url из вызова ---
        processor = DetailsProcessor(session, db_pool)
        
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        async def worker(url, breadcrumbs):
            async with semaphore:
                await processor.process_item(url, breadcrumbs)

        tasks = []
        for filename in os.listdir(URLS_DIR):
            if filename.endswith('.json'):
                filepath = os.path.join(URLS_DIR, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    breadcrumbs = data.get('breadcrumbs', ['Без категории'])
                    for url in data['product_urls']:
                        tasks.append(worker(url, breadcrumbs))
        
        print(f"🚀 Launching processing for {len(tasks)} products with a concurrency limit of {CONCURRENCY_LIMIT}...")
        await asyncio.gather(*tasks)

    if db_pool:
        await db_pool.close()