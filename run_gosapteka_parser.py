import asyncio
import os
import shutil
import sys

# ИЗМЕНЕНО: Правильные импорты из файлов с новыми именами
from parsers.gosapteka.url_collector import collect_urls_to_files
from parsers.gosapteka.details_processor import process_details_from_files
from config import URLS_DIR

async def main():
    """
    Главный скрипт для последовательного или раздельного запуска парсеров.
    Принимает аргументы: 'stage1', 'stage2', 'full'.
    """
    stage = sys.argv[1] if len(sys.argv) > 1 else 'full'

    if stage in ['stage1', 'full']:
        print("="*50)
        print("▶️ STAGE 1: COLLECTING URLS AND SAVING TO FILES")
        print("="*50)

        if os.path.exists(URLS_DIR):
            shutil.rmtree(URLS_DIR)
            print(f"🧹 Directory '{URLS_DIR}' cleaned.")
        os.makedirs(URLS_DIR)

        await collect_urls_to_files()

        print("\n" + "="*50)
        print("✅ STAGE 1 COMPLETE.")

    if stage in ['stage2', 'full']:
        print("\n▶️ STAGE 2: PROCESSING DETAILS FROM FILES AND SAVING TO DB")
        print("="*50)

        try:
            await process_details_from_files()
        except Exception as e:
            print(f"❌ A critical error occurred during Stage 2: {e}")

        print("\n" + "="*50)
        print("✅ STAGE 2 COMPLETE.")

    if stage not in ['stage1', 'stage2', 'full']:
        print(f"❌ Invalid argument '{stage}'. Use 'stage1', 'stage2', or 'full'.")
        return

    print("\n🎉 ALL STAGES COMPLETE. WORK FINISHED.")

if __name__ == "__main__":
    asyncio.run(main())