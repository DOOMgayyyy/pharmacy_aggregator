import asyncio
import json
import random
import time

from playwright.async_api import async_playwright, TimeoutError
from bs4 import BeautifulSoup

# --- НОВАЯ И УЛУЧШЕННАЯ КОНФИГУРАЦИЯ ---
# Количество одновременно обрабатываемых категорий. Уменьшено для снижения нагрузки.
CONCURRENCY_LIMIT = 3
# Количество повторных попыток для загрузки страницы
RETRY_COUNT = 3
# Начальная задержка перед повторной попыткой (в секундах). Будет увеличиваться.
RETRY_DELAY = 5
# Базовый URL сайта
BASE_URL = "https://planetazdorovo.ru"


# --- Хелпер-функция для парсинга продуктов со страницы (без изменений) ---
def scrape_products_from_page(page_content, base_url):
    """Парсит HTML-контент для извлечения данных о продуктах."""
    soup = BeautifulSoup(page_content, 'html.parser')
    products = []
    product_cards = soup.select('div.pz-grid-list .pz-grid-item .item-card')

    for card in product_cards:
        link_tag = card.select_one('.item-card-title-text')
        title_tag = card.select_one('.item-card-title-text .this-full')
        price_tag = card.select_one('.item-card-price-number')
        availability_tag = card.select_one('.item-card-availability-text .this-text-number')

        if link_tag and title_tag:
            relative_link = link_tag.get('href', '')
            full_link = f"{base_url}{relative_link}" if relative_link else "N/A"

            product_data = {
                'title': title_tag.get_text(strip=True),
                'price': price_tag.get_text(strip=True).replace('\n', '').replace(' ', '') if price_tag else "N/A",
                'availability': availability_tag.get_text(strip=True) if availability_tag else "N/A",
                'link': full_link
            }
            products.append(product_data)

    return products


# --- Новые и обновлённые асинхронные функции ---

async def goto_with_retries(page, url: str) -> bool:
    """
    Переходит по URL с несколькими попытками в случае неудачи.
    Использует экспоненциальную задержку между попытками.
    """
    for i in range(RETRY_COUNT):
        try:
            # Увеличиваем общий таймаут на загрузку до 90 секунд
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            return True # Успешная загрузка
        except TimeoutError as e:
            print(f"⚠️ Попытка {i + 1}/{RETRY_COUNT} не удалась для {url}. Ошибка: {e.message.splitlines()[0]}")
            if i < RETRY_COUNT - 1:
                # Увеличивающаяся задержка (5s, 10s, 15s...)
                delay = RETRY_DELAY * (i + 1)
                print(f"   ...пауза на {delay} секунд перед повторной попыткой...")
                await asyncio.sleep(delay)
    print(f"❌ Не удалось загрузить страницу {url} после {RETRY_COUNT} попыток.")
    return False # Все попытки провалились

async def get_last_page_number(page) -> int:
    """Находит номер последней страницы в пагинации."""
    try:
        page_links = await page.query_selector_all("div.pagination a.pagination__item")
        if not page_links:
            return 1
        last_page_number = 0
        for link in page_links:
            text = await link.inner_text()
            if text.isdigit() and int(text) > last_page_number:
                last_page_number = int(text)
        print(f"INFO: Обнаружено страниц в категории: {last_page_number}")
        return last_page_number if last_page_number > 0 else 1
    except Exception as e:
        print(f"WARNING: Не удалось определить последнюю страницу: {e}. Будет обработана только первая.")
        return 1

async def scroll_to_bottom(page):
    """Прокручивает страницу до самого низа для загрузки всех 'ленивых' элементов."""
    last_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        try:
            await page.wait_for_load_state('networkidle', timeout=7000)
        except TimeoutError:
            print("...Тайм-аут ожидания 'networkidle', продолжаю прокрутку...")
        await asyncio.sleep(random.uniform(1.5, 2.5))
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

async def scrape_single_category(context, category_url: str):
    """Полностью скрейпит одну категорию, включая все ее страницы."""
    print(f"\n--- 🚀 Начинаю обработку категории: {category_url} ---")
    page = await context.new_page()
    all_products_in_category = []

    try:
        # 1. Переходим на первую страницу с использованием новой надежной функции
        if not await goto_with_retries(page, category_url):
            raise Exception("Не удалось загрузить даже первую страницу категории.")

        await page.wait_for_selector('div.pz-grid-list', timeout=30000)
        last_page = await get_last_page_number(page)

        # 2. Итерируемся по всем страницам
        for page_num in range(1, last_page + 1):
            page_url = f"{category_url}?PAGEN_1={page_num}"
            print(f"📖 Обрабатываю страницу {page_num}/{last_page} для категории: {category_url}")

            # Для первой страницы мы уже на месте, для остальных - переходим
            if page_num > 1:
                if not await goto_with_retries(page, page_url):
                    print(f"   Пропускаю страницу {page_num} из-за ошибки загрузки.")
                    continue # Пропускаем страницу, но не всю категорию

            await page.wait_for_selector('div.pz-grid-list', timeout=30000)
            
            print("...Прокручиваю страницу для загрузки всех товаров...")
            await scroll_to_bottom(page)
            print("...Прокрутка завершена.")

            content = await page.content()
            products_on_page = scrape_products_from_page(content, BASE_URL)
            all_products_in_category.extend(products_on_page)
            print(f"✅ Найдено {len(products_on_page)} товаров на странице {page_num}.")
            # Увеличена задержка между страницами
            await asyncio.sleep(random.uniform(2.0, 4.0))

    except Exception as e:
        print(f"❌ CRITICAL ERROR при обработке {category_url}: {e}")
    finally:
        await page.close()
    
    print(f"--- ✅ Завершено: {category_url} | Собрано товаров: {len(all_products_in_category)} ---")
    return all_products_in_category

# --- Главная асинхронная функция (без изменений, кроме вызова `main`) ---
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--start-maximized"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )

        page = await context.new_page()
        
        print("--- Шаг 1: Сбор ссылок на категории ---")
        catalog_url = f"{BASE_URL}/catalog/"
        await page.goto(catalog_url, wait_until="domcontentloaded")
        await page.wait_for_selector("div.catalog", timeout=30000)

        category_links = await page.eval_on_selector_all(
            'div.catalog a.catalog__card',
            'nodes => nodes.map(n => n.href)'
        )
        await page.close()
        
        print(f"Найдено {len(category_links)} категорий для скрейпинга.")

        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = []

        async def worker(link):
            async with semaphore:
                # Добавляем паузу перед началом обработки новой категории
                await asyncio.sleep(random.uniform(3.0, 6.0))
                return await scrape_single_category(context, link)

        for link in category_links:
            tasks.append(worker(link))
            
        results_from_categories = await asyncio.gather(*tasks)

        all_products = [product for sublist in results_from_categories for product in sublist]
        
        print(f"\n--- ✨ Скрейпинг полностью завершен ---")
        print(f"🎉 Всего собрано товаров: {len(all_products)}")

        if all_products:
            with open("all_products_async.json", "w", encoding="utf-8") as f:
                json.dump(all_products, f, indent=4, ensure_ascii=False)
            print("💾 Все данные сохранены в файл 'all_products_async.json'")
        
        await browser.close()
        print("Браузер закрыт.")

if __name__ == "__main__":
    asyncio.run(main())