# api.py
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
from config import DB_CONFIG
from parsers.base_parser import light_normalize # <-- 1. Импортируем правильную функцию

app = FastAPI(
    title="Pharmacy Aggregator API",
    description="API для получения данных о лекарствах и ценах из разных аптек.",
    version="1.0.0"
)

# --- Middleware для CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Разрешает запросы с любых доменов
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Подключение к статическим файлам (для изображений) ---
# Теперь можно будет открывать картинки по ссылке http://127.0.0.1:8000/static/images/products/image.jpg
app.mount("/static", StaticFiles(directory="static"), name="static")


# --- Управление подключением к БД ---
db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG)
        print("✅ API подключено к базе данных.")
    except Exception as e:
        print(f"❌ API не удалось подключиться к базе данных: {e}")

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
        print("🔌 API отключено от базы данных.")


# --- Эндпоинты API ---

@app.get("/search", tags=["Medicines"])
async def search_medicines(q: str = ""):
    """
    Умный поиск лекарств по названию.
    Сравнивает нормализованный поисковый запрос с полем `name` в базе.
    """
    if not q or len(q) < 3:
        return {"message": "Поисковый запрос должен содержать не менее 3 символов."}
    
    search_term = light_normalize(q) # <-- 2. Используем light_normalize
    async with db_pool.acquire() as conn:
        # <-- 3. Ищем по полю `name`, а не `normalized_name`
        # Дополнительно считаем, в скольких аптеках есть товар (pharmacy_count)
        query = """
        SELECT 
            m.id, 
            m.name, 
            m.image_url, 
            MIN(p.price) as min_price,
            COUNT(p.pharmacy_id) as pharmacy_count
        FROM medicines m
        JOIN pharmacy_prices p ON m.id = p.medicine_id
        WHERE similarity(m.name, $1) > 0.2
        GROUP BY m.id
        ORDER BY similarity(m.name, $1) DESC
        LIMIT 20;
        """
        results = await conn.fetch(query, search_term)
        return [dict(r) for r in results]

@app.get("/medicine/{medicine_id}", tags=["Medicines"])
async def get_medicine_details(medicine_id: int):
    """
    Получает детальную информацию о лекарстве и список цен в разных аптеках.
    """
    async with db_pool.acquire() as conn:
        medicine = await conn.fetchrow("SELECT * FROM medicines WHERE id = $1", medicine_id)
        if not medicine:
            raise HTTPException(status_code=404, detail="Лекарство не найдено")
        
        prices = await conn.fetch("""
            SELECT p.price, p.last_updated, ph.name AS pharmacy_name
            FROM pharmacy_prices p
            JOIN pharmacies ph ON p.pharmacy_id = ph.id
            WHERE p.medicine_id = $1 ORDER BY p.price;
        """, medicine_id)
        
        return {"details": dict(medicine), "prices": [dict(p) for p in prices]}

# --- ОБНОВЛЕННЫЕ ЭНДПОИНТЫ ДЛЯ ИЕРАРХИИ КАТЕГОРИЙ ---

@app.get("/categories", tags=["Categories"])
async def get_root_categories():
    """
    Возвращает только категории верхнего уровня (у которых нет родителя).
    """
    async with db_pool.acquire() as conn:
        # <-- 4. Запрашиваем из новой таблицы `categories`
        query = "SELECT id, name FROM categories WHERE parent_id IS NULL ORDER BY name;"
        results = await conn.fetch(query)
        return [dict(r) for r in results]

@app.get("/categories/{category_id}", tags=["Categories"])
async def get_category_children(category_id: int):
    """
    Возвращает дочерние категории для указанного ID родительской категории.
    """
    async with db_pool.acquire() as conn:
        query = "SELECT id, name FROM categories WHERE parent_id = $1 ORDER BY name;"
        results = await conn.fetch(query, category_id)
        return [dict(r) for r in results]