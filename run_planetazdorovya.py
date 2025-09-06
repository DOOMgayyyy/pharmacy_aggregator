# run_planetazdorovya.py

import asyncio
import asyncpg
import sys

# --- Import configurations ---
from config import DB_CONFIG
from parsers.planeta_zdorovya.planeta_zdorovya_parser import PlanetaZdorovyaParser
# Правильная строка
from parsers.base_parser import BaseParser, light_normalize

# --- Source File ---
# Note: You could also move this path to your config.py if you prefer
JSON_FILE_PATH = "parsers/planeta_zdorovya/all_products_async.json"

async def main():
    """
    Main script to run the 'Planeta Zdorovya' parser.
    Accepts arguments: 'stage1' (populate medicines), 'stage2' (parse prices), or 'full'.
    """
    stage = sys.argv[1] if len(sys.argv) > 1 else 'full'

    # Establish database connection using the imported config
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    
    parser = PlanetaZdorovyaParser(db_pool)

    # --- STAGE 1: Populate the master 'medicines' table ---
    if stage in ['stage1', 'full']:
        print("="*50)
        print("▶️ ЭТАП 1: ЗАПОЛНЕНИЕ ГЛАВНОГО КАТАЛОГА ЛЕКАРСТВЕННЫХ СРЕДСТВ (Планета Здоровья)")
        print("="*50)

        await parser.populate_medicines_from_json(JSON_FILE_PATH)
        
        print("\n" + "="*50)
        print("✅ STAGE 1 COMPLETE.")

    # --- STAGE 2: Parse prices and link them to medicines ---
    if stage in ['stage2', 'full']:
        print("\n▶️ ЭТАП 2: РАЗБОР ЦЕН (Планета Здоровья)")
        print("="*50)
        
        try:
            await parser.parse_prices_from_json(JSON_FILE_PATH)
        except Exception as e:
            print(f"❌ На этапе 2 произошла критическая ошибка: {e}")

        print("\n" + "="*50)
        print("✅ ЭТАП 2 ЗАВЕРШЕН.")

    # --- Argument validation ---
    if stage not in ['stage1', 'stage2', 'full']:
        print(f"❌ Invalid argument '{stage}'. Use 'stage1', 'stage2', or 'full'.")
        await db_pool.close()
        return

    # --- Clean up and exit ---
    await db_pool.close()
    print("\n🎉 ВСЕ ЭТАПЫ СОЗДАНИЯ 'ПЛАНЕТЫ ЗДОРОВЬЯ' ЗАВЕРШЕНЫ. РАБОТА ЗАВЕРШЕНА.")

if __name__ == "__main__":
    asyncio.run(main())