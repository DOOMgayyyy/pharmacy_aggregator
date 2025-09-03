# 3_download_images.py
import asyncio
import os
import asyncpg
import httpx
from PIL import Image
from config import DB_CONFIG, IMAGES_DIR, CONCURRENCY_LIMIT

class ImageDownloader:
    def __init__(self, session: httpx.AsyncClient, db_pool: asyncpg.Pool):
        self.session = session
        self.db_pool = db_pool

    async def find_images_to_download(self) -> list[asyncpg.Record]:
        async with self.db_pool.acquire() as conn:
            return await conn.fetch("SELECT id, image_url FROM medicines WHERE image_url LIKE 'http%'")

    async def process_image(self, medicine_id: int, image_url: str):
        try:
            image_name = os.path.basename(urlparse(image_url).path)
            local_path = os.path.join(IMAGES_DIR, image_name)
            os.makedirs(IMAGES_DIR, exist_ok=True)
            
            response = await self.session.get(image_url, timeout=30)
            response.raise_for_status()

            with Image.open(response.iter_bytes()) as img:
                img.save(local_path, format=img.format or 'JPEG', quality=85)
            
            await self.update_db_path(medicine_id, local_path)
            print(f"✅ Изображение для ID {medicine_id} сохранено: {local_path}")
        except Exception as e:
            print(f"❌ Ошибка для {image_url}: {e}")

    async def update_db_path(self, med_id: int, path: str):
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE medicines SET image_url = $1 WHERE id = $2", path, med_id)

async def main():
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    headers = {'User-Agent': 'Mozilla/5.0'}
    async with httpx.AsyncClient(headers=headers) as session:
        downloader = ImageDownloader(session, db_pool)
        records = await downloader.find_images_to_download()

        if not records:
            print("🤷 Нет новых изображений.")
            return

        print(f"🖼️  Найдено {len(records)} изображений для загрузки.")
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def worker(record):
            async with semaphore:
                await downloader.process_image(record['id'], record['image_url'])

        await asyncio.gather(*[worker(r) for r in records])

    await db_pool.close()
    print("\n🎉 Загрузка изображений завершена.")

if __name__ == "__main__":
    from urllib.parse import urlparse
    asyncio.run(main())