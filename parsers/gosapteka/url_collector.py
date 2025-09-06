# parsers/gosapteka/url_collector.py
import asyncio
import json
import os
import random
from urllib.parse import urljoin
import httpx
from bs4 import BeautifulSoup, Tag
from ..base_parser import BaseParser
from config import CONCURRENCY_LIMIT, DELAY_BETWEEN_PAGES, DELAY_BETWEEN_CATEGORIES, URLS_DIR

class UrlCollector(BaseParser):
    # ... (all class methods like _recursive_parse_menu, _get_category_structure, etc.)
    """Parses the category structure and collects all product URLs."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parsed_links = set()

    def _recursive_parse_menu(self, element: Tag, breadcrumbs: list):
        """Recursively parses the menu to build a category tree."""
        results = []
        item_classes = ['menu-catalog__item', 'menu-catalog__sub-item']
        link_classes = ['menu-catalog__link', 'menu-catalog__sub-link']
        submenu_classes = ['menu-catalog__sub-menu', 'menu-catalog__sub2-menu']

        for item in element.find_all(lambda tag: any(c in tag.get('class', []) for c in item_classes), recursive=False):
            link = item.find('a', class_=link_classes, recursive=False)
            if not link or not link.text.strip():
                continue
            
            name = link.text.strip()
            url = urljoin(self.base_url, link.get('href', ''))
            current_breadcrumbs = breadcrumbs + [name]
            
            submenu = item.find('div', class_=submenu_classes, recursive=False)
            if submenu:
                results.extend(self._recursive_parse_menu(submenu, current_breadcrumbs))
            else:
                results.append({'url': url, 'breadcrumbs': current_breadcrumbs})
        return results

    async def _get_category_structure(self) -> list:
        """Gets the full hierarchical category structure from the main page."""
        print("▶️ Parsing category structure...")
        html = await self.fetch_html(self.base_url + '/')
        if not html: return []

        soup = BeautifulSoup(html, 'html.parser')
        catalog_container = soup.find('div', class_='menu-catalog')
        if not catalog_container: return []

        all_categories = self._recursive_parse_menu(catalog_container, [])
        print(f"✅ Found {len(all_categories)} final categories.")
        return all_categories

    def _extract_links_from_page(self, html: str) -> list[str]:
        """Extracts product links from a category page."""
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('a.product-mini__title-link, a.product-mini__picture')
        return list(set(urljoin(self.base_url, link.get('href')) for link in links if link.get('href')))

    def _find_next_page(self, html: str) -> str | None:
        """Finds the link to the next page in the pagination."""
        soup = BeautifulSoup(html, 'html.parser')
        next_btn = soup.find('a', class_='modern-page-next')
        return urljoin(self.base_url, next_btn['href']) if next_btn and next_btn.get('href') else None

    async def _parse_single_category(self, category_info: dict):
        """Parses all pages of a single category and saves the links to a file."""
        start_url = category_info['url']
        breadcrumbs = category_info['breadcrumbs']
        print(f"\n🚀 Parsing category: {' -> '.join(breadcrumbs)}")
        
        current_url, page_num = start_url, 1
        category_links = []
        while current_url:
            print(f"📖 Page #{page_num}: {current_url}")
            html = await self.fetch_html(current_url)
            if not html: break

            page_links = self._extract_links_from_page(html)
            new_links = [link for link in page_links if link not in self.parsed_links]
            
            if not new_links:
                print("⛔ No new products found, finishing category.")
                break
            
            category_links.extend(new_links)
            self.parsed_links.update(new_links)
            
            current_url = self._find_next_page(html)
            page_num += 1
            await asyncio.sleep(random.uniform(*DELAY_BETWEEN_PAGES))
        
        if category_links:
            self._save_results(category_links, start_url, breadcrumbs)

    def _save_results(self, links: list, url: str, breadcrumbs: list[str]):
        """Saves the found links and their category path to a JSON file."""
        slug = url.strip('/').split('/')[-1] or "home"
        filepath = os.path.join(URLS_DIR, f"{slug}.json")
        output = {'category_url': url, 'breadcrumbs': breadcrumbs, 'product_urls': sorted(links)}
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved {len(links)} URLs for '{breadcrumbs[-1]}' to {filepath}")


async def collect_urls_to_files():
    """Main orchestrator function for collecting URLs."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        collector = UrlCollector(session)
        categories = await collector._get_category_structure()

        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        async def worker(cat_info):
            async with semaphore:
                await collector._parse_single_category(cat_info)
                await asyncio.sleep(random.uniform(*DELAY_BETWEEN_CATEGORIES))

        await asyncio.gather(*[worker(cat) for cat in categories])