# 2_run_price_parse.py
import asyncio
import httpx
import asyncpg
from config import DB_CONFIG, CONCURRENCY_LIMIT
from parsers.planeta_zdorovya.price_parser import PlanetaZdorovyaPriceParser

async def main():
    # Эти URL должны быть собраны соответствующим url_collector'ом
    urls_to_parse = [
        'https://planetazdorovo.ru/catalog/lekarstva-i-bad/nurofen-forte-tabletki-400mg-n12/',
        'https://planetazdorovo.ru/catalog/lekarstva-i-bad/aspirin-kardio-tabletki-100mg-n28/',
    ]

    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        parser = PlanetaZdorovyaPriceParser(session, db_pool)
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def worker(url):
            async with semaphore:
                await parser.process_price(url)

        tasks = [worker(url) for url in urls_to_parse]
        await asyncio.gather(*tasks)

    await db_pool.close()
    print("\n\n🎉 Сбор цен завершен!")

if __name__ == "__main__":
    asyncio.run(main())