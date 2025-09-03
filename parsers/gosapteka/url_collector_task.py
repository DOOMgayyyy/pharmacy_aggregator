# parsers/gosapteka/url_collector_task.py
import asyncio
import json
import os
import random
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse, urlencode
import httpx
from bs4 import BeautifulSoup
from config import CONCURRENCY_LIMIT, DELAY_BETWEEN_PAGES, DELAY_BETWEEN_CATEGORIES, URLS_DIR

# --- БАЗОВЫЙ КЛАСС (Обычно выносится, но здесь для полноты) ---
class GosAptekaParser:
    """Базовый класс для парсеров Госаптеки."""
    def __init__(self, session: httpx.AsyncClient):
        self.base_url = 'https://gosapteka18.ru'
        self.session = session

    async def fetch_html(self, url: str) -> str | None:
        try:
            await asyncio.sleep(0.5)
            response = await self.session.get(url, timeout=20, follow_redirects=True)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            print(f"🚫 Ошибка загрузки {url}: {e}")
            return None

# --- КЛАСС ДЛЯ СБОРА СТРУКТУРЫ КАТЕГОРИЙ ---
class CategoryStructureParser(GosAptekaParser):
    """Класс для асинхронного сбора структуры категорий с сайта."""
    async def parse_structure(self) -> dict:
        """
        Собирает все категории и подкатегории с главной страницы.
        Возвращает словарь со структурой каталога.
        """
        print("▶️ Шаг 1: Асинхронный парсинг структуры категорий...")
        main_page_url = self.base_url + '/'
        html = await self.fetch_html(main_page_url)
        if not html:
            return {'error': 'Не удалось загрузить главную страницу'}

        try:
            soup = BeautifulSoup(html, 'html.parser')
            catalog_container = soup.find('div', class_='menu-catalog')
            if not catalog_container:
                return {'error': "Контейнер каталога 'menu-catalog' не найден"}

            structured_categories = {}
            columns = catalog_container.find_all('div', class_='menu-catalog__list')
            if not columns:
                return {'error': "Колонки категорий 'menu-catalog__list' не найдены"}

            for col in columns:
                items = col.find_all('div', class_='menu-catalog__item', recursive=False)
                for item in items:
                    parent_link = item.find('a', class_='menu-catalog__link')
                    if not parent_link or not parent_link.text.strip():
                        continue

                    parent_name = parent_link.text.strip()
                    parent_url = self.base_url + parent_link.get('href', '')
                    subcategories_l1 = []
                    submenu_l1 = item.find('div', class_='menu-catalog__sub-menu')
                    
                    if submenu_l1:
                        subitems_l1 = submenu_l1.find_all('div', class_='menu-catalog__sub-item')
                        for subitem in subitems_l1:
                            sub_link = subitem.find('a', class_='menu-catalog__sub-link')
                            if not sub_link: continue
                            
                            sub_name = sub_link.text.strip()
                            sub_url = self.base_url + sub_link.get('href', '')
                            subcategories_l2 = []
                            submenu_l2 = subitem.find('div', class_='menu-catalog__sub2-menu')
                            
                            if submenu_l2:
                                for sub2_link in submenu_l2.find_all('a', class_='menu-catalog__sub2-link'):
                                    subcategories_l2.append({
                                        'name': sub2_link.text.strip(),
                                        'url': self.base_url + sub2_link.get('href', '')
                                    })
                            
                            subcategories_l1.append({
                                'name': sub_name, 'url': sub_url, 'subcategories': subcategories_l2
                            })
                    
                    structured_categories[parent_name] = {'url': parent_url, 'subcategories': subcategories_l1}
            
            print("✅ Структура категорий успешно собрана.")
            return structured_categories
        except Exception as e:
            print(f"❌ Произошла непредвиденная ошибка при парсинге категорий: {e}")
            return {"error": str(e)}

# --- КЛАСС ДЛЯ СБОРА ССЫЛОК НА ТОВАРЫ ---
class ProductLinkParser(GosAptekaParser):
    """
    Парсер для асинхронного сбора всех товаров в категории с учетом пагинации.
    """
    def __init__(self, session: httpx.AsyncClient):
        super().__init__(session)
        self.parsed_links = set()
        self.first_page_content = None

    def extract_links_from_page(self, html: str) -> list[str]:
        """Извлекает ссылки на товары из HTML страницы."""
        if not html: return []
        
        soup = BeautifulSoup(html, 'html.parser')
        products_grid = soup.find('div', class_='products__grid')
        if not products_grid: return []
        
        links = products_grid.select('a.product-mini__title-link, a.product-mini__picture')
        
        return list(set([urljoin(self.base_url, link.get('href')) for link in links if link.get('href')]))

    def find_next_page(self, html: str, current_url: str, current_page: int) -> str | None:
        """Находит URL следующей страницы, используя 3 метода."""
        soup = BeautifulSoup(html, 'html.parser')
        
        next_btn = soup.find('a', class_='modern-page-next')
        if next_btn and next_btn.get('href'):
            return urljoin(self.base_url, next_btn.get('href'))
        
        page_links = soup.select('a.pagination__item, a.bx-pagination-container a')
        for link in page_links:
            try:
                if int(link.text.strip()) == current_page + 1:
                    return urljoin(self.base_url, link.get('href'))
            except (ValueError, TypeError):
                continue
        
        parsed_url = urlparse(current_url)
        query_params = parse_qs(parsed_url.query)
        query_params['PAGEN_1'] = [str(current_page + 1)]
        new_query = urlencode(query_params, doseq=True)
        return urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, new_query, parsed_url.fragment))

    async def parse_products_from_category(self, start_url: str, category_name: str):
        """Асинхронно парсит все страницы категории и собирает ссылки на товары."""
        print(f"\n🚀 Начинаю парсинг категории '{category_name}': {start_url}")
        
        current_url = start_url
        page_num = 1
        all_links_for_category = []
        
        while current_url:
            print(f"📖 Страница #{page_num}: {current_url}")
            
            html = await self.fetch_html(current_url)
            if not html:
                print("Прерываю парсинг этой категории из-за ошибки загрузки.")
                break

            if page_num == 1:
                self.first_page_content = html

            page_links = self.extract_links_from_page(html)
            new_links = [link for link in page_links if link not in self.parsed_links]
            print(f"Найдено товаров: {len(page_links)}, из них новых: {len(new_links)}")
            
            if not page_links or (page_num > 1 and not new_links) or (page_num > 1 and html == self.first_page_content):
                if not page_links or (page_num > 1 and not new_links):
                    print("⛔ Новых товаров не найдено или страница пуста, завершаю.")
                else:
                    print("⚠️  Достигнут конец пагинации (контент дублирует первую страницу).")
                break

            all_links_for_category.extend(new_links)
            self.parsed_links.update(new_links)
            
            next_url = self.find_next_page(html, current_url, page_num)
            
            if not next_url or next_url == current_url:
                print("➡️  Следующая страница не найдена, завершаю парсинг категории.")
                break
            
            current_url = next_url
            page_num += 1
            delay = random.uniform(*DELAY_BETWEEN_PAGES)
            await asyncio.sleep(delay)
        
        if all_links_for_category:
            self.save_results(all_links_for_category, start_url, category_name)
        print(f"✅ Парсинг категории '{category_name}' завершен! Всего найдено уникальных товаров: {len(all_links_for_category)}")
        return all_links_for_category

    def save_results(self, links: list, base_url: str, category_human_name: str):
        """Сохраняет ссылки и URL категории в JSON-файл."""
        category_slug = urlparse(base_url).path.strip('/').split('/')[-1] or "home"
        os.makedirs(URLS_DIR, exist_ok=True)
        filepath = os.path.join(URLS_DIR, f"{category_slug}.json")

        output_data = {
            'category_url': base_url,
            'category_name': category_human_name,
            'product_urls': sorted(list(set(links)))
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"💾 Результаты для '{category_human_name}' сохранены в JSON: {filepath}")


# --- ГЛАВНАЯ ФУНКЦИЯ-ОРКЕСТРАТОР ---
async def collect_urls_to_files():
    """Основная функция для запуска парсинга и сохранения URL в файлы."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        structure_parser = CategoryStructureParser(session)
        catalog_data = await structure_parser.parse_structure()

        if 'error' in catalog_data or not catalog_data:
            print(f"🚫 КРИТИЧЕСКАЯ ОШИБКА: {catalog_data.get('error', 'Не удалось собрать структуру категорий.')}")
            return
        
        with open('categories_structure.json', 'w', encoding='utf-8') as f:
            json.dump(catalog_data, f, ensure_ascii=False, indent=4)
        print("\n💾 Полная структура категорий сохранена в 'categories_structure.json'\n" + "="*50)

        product_parser = ProductLinkParser(session)
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = []

        async def worker(cat_url, sub_cat_name):
            async with semaphore:
                print(f"✨ Запускаю задачу для подкатегории: '{sub_cat_name}'")
                await product_parser.parse_products_from_category(cat_url, sub_cat_name)
                delay = random.uniform(*DELAY_BETWEEN_CATEGORIES)
                print(f"--- Пауза {delay:.2f} сек. перед следующей категорией ---")
                await asyncio.sleep(delay)
        
        for parent_data in catalog_data.values():
            for subcategory in parent_data.get('subcategories', []):
                # Проверяем, есть ли у подкатегории свои подкатегории (3-й уровень)
                if subcategory.get('subcategories'):
                    for sub_sub_cat in subcategory['subcategories']:
                        tasks.append(asyncio.create_task(worker(sub_sub_cat['url'], sub_sub_cat['name'])))
                else:
                    # Если нет, парсим саму подкатегорию 2-го уровня
                    tasks.append(asyncio.create_task(worker(subcategory['url'], subcategory['name'])))
        
        print(f"\n🔥 Сформировано {len(tasks)} задач для парсинга. Запускаю...")
        await asyncio.gather(*tasks)

    print("\n\n🎉 Все задачи по сбору URL выполнены!.")

# Эта часть нужна, если вы захотите запустить этот файл отдельно для отладки
if __name__ == "__main__":
    # Создаем директорию, если ее нет
    if not os.path.exists(URLS_DIR):
        os.makedirs(URLS_DIR)
        print(f"Создана директория: {URLS_DIR}")
        
    asyncio.run(collect_urls_to_files())