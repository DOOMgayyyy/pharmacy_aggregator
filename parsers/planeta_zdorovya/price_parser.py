# parsers/planeta_zdorovya/price_parser.py
from bs4 import BeautifulSoup
import re
from ..base_parser import BaseParser, normalize_name

class PlanetaZdorovyaPriceParser(BaseParser):
    """Парсит ТОЛЬКО ЦЕНЫ и привязывает их к эталонному каталогу."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = 'https://planetazdorovo.ru'
        self.pharmacy_name = "Планета Здоровья"

    async def get_pharmacy_id(self) -> int:
        async with self.db_pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO pharmacies (name, address) VALUES ($1, $2)
                ON CONFLICT (address) DO UPDATE SET name = EXCLUDED.name RETURNING id;
                """,
                self.pharmacy_name, self.base_url
            )

    def _parse_data(self, soup: BeautifulSoup) -> tuple[str | None, float | None]:
        title = soup.select_one('h1.product-title') # Пример, селектор нужно проверить
        price = soup.select_one('div.product-price span.price') # Пример
        price_text = "".join(re.findall(r'\d', price.get_text())) if price else ""
        return (title.get_text(strip=True) if title else None, float(price_text) / 100 if price_text else None)

    async def process_price(self, product_url: str):
        print(f"💰 Поиск цены: {product_url}")
        html = await self.fetch_html(product_url)
        if not html: return

        title, price = self._parse_data(BeautifulSoup(html, 'html.parser'))
        if not title or price is None: return

        search_term = normalize_name(title)
        async with self.db_pool.acquire() as conn:
            medicine_id = await conn.fetchval("SELECT id FROM medicines WHERE similarity(normalized_name, $1) > 0.4 ORDER BY similarity(normalized_name, $1) DESC LIMIT 1;", search_term)

            if not medicine_id:
                print(f"🤷‍♂️ Не найден эталон для '{title}'")
                return

            pharmacy_id = await self.get_pharmacy_id()
            await conn.execute("""
                INSERT INTO pharmacy_prices (pharmacy_id, medicine_id, price) VALUES ($1, $2, $3)
                ON CONFLICT (pharmacy_id, medicine_id) DO UPDATE SET price = EXCLUDED.price, last_updated = NOW();
            """, pharmacy_id, medicine_id, price)
            print(f"💵 Найдена цена для '{title}': {price} руб.")