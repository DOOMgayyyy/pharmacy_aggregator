# python run_gosapteka_parser.py stage1 — запустит только сбор URL-адресов.

# python run_gosapteka_parser.py stage2 — запустит только обработку деталей из уже собранных файлов.

# python run_gosapteka_parser.py full — запустит оба этапа последовательно (как и раньше).


# run_gosapteka_parser.py
import asyncio
import os
import shutil
import sys  # Импортируем модуль для работы с аргументами командной строки

from parsers.gosapteka.url_collector_task import collect_urls_to_files
from parsers.gosapteka.details_processor_task import process_details_from_files
from config import URLS_DIR

async def main():
    """
    Главный скрипт для последовательного или раздельного запуска парсеров.
    Принимает аргументы: 'stage1', 'stage2', 'full'.
    """
    # Получаем аргумент из командной строки (по умолчанию 'full')
    stage = sys.argv[1] if len(sys.argv) > 1 else 'full'

    if stage in ['stage1', 'full']:
        print("="*50)
        print("▶️ ЭТАП 1: СБОР URL-АДРЕСОВ И СОХРАНЕНИЕ В ФАЙЛЫ")
        print("="*50)
        
        # Очищаем директорию только перед началом сбора URL
        if os.path.exists(URLS_DIR):
            shutil.rmtree(URLS_DIR)
            print(f"🧹 Директория '{URLS_DIR}' очищена.")
        
        await collect_urls_to_files()
        print("\n" + "="*50)
        print("✅ ЭТАП 1 ЗАВЕРШЕН.")

    if stage in ['stage2', 'full']:
        print("\n▶️ ЭТАП 2: ОБРАБОТКА ДЕТАЛЕЙ ИЗ ФАЙЛОВ И СОХРАНЕНИЕ В БД")
        print("="*50)

        await process_details_from_files()
        print("\n" + "="*50)
        print("✅ ЭТАП 2 ЗАВЕРШЕН.")

    if stage not in ['stage1', 'stage2', 'full']:
        print(f"❌ Неверный аргумент '{stage}'. Используйте 'stage1', 'stage2' или 'full'.")
        return

    print("\n🎉 РАБОТА ОКОНЧЕНА.")

if __name__ == "__main__":
    asyncio.run(main())