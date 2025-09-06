import asyncio
import os
import glob
import json
import httpx
import asyncpg
from config import DB_CONFIG, CONCURRENCY_LIMIT
from parsers.gosapteka.details_processor import DetailsProcessor

async def main():
    """
    Finds the latest error log, reads the failed URLs,
    and attempts to process them again.
    """
    # Find the most recent error log file
    error_logs = glob.glob('log_error_*.json')
    if not error_logs:
        print("🤷 No error logs found. Nothing to retry.")
        return
    
    latest_log = max(error_logs, key=os.path.getctime)
    print(f"🔁 Retrying failed URLs from: {latest_log}")

    try:
        with open(latest_log, 'r', encoding='utf-8') as f:
            failed_items = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print(f"❌ Could not read or parse {latest_log}.")
        return

    if not failed_items:
        print(f"✅ Log file '{latest_log}' is empty. Nothing to do.")
        return

    # --- Standard setup for the parser ---
    db_pool = None
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG)
        print("✅ Database connection established.")
    except Exception as e:
        print(f"❌ Critical Error: Could not connect to the database: {e}")
        return

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as session:
        processor = DetailsProcessor(session, db_pool)
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def worker(item):
            async with semaphore:
                # The processor's log_error method will automatically handle
                # URLs that fail again, adding them to today's log.
                await processor.process_item(item['url'], item['breadcrumbs'])

        tasks = [worker(item) for item in failed_items]
        print(f"🚀 Relaunching processing for {len(tasks)} failed items...")
        await asyncio.gather(*tasks)

    if db_pool:
        await db_pool.close()

    print("\n🎉 Retry process finished.")

if __name__ == "__main__":
    asyncio.run(main())