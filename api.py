from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
from config import DB_CONFIG
from parsers.base_parser import normalize_name

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(**DB_CONFIG)

@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()

@app.get("/search")
async def search_medicines(q: str = ""):
    """УМНЫЙ ПОИСК: использует pg_trgm для поиска по нормализованным именам."""
    if not q: return []
    search_term = normalize_name(q)
    async with db_pool.acquire() as conn:
        query = """
        SELECT m.id, m.name, m.description, m.image_url, MIN(p.price) as min_price
        FROM medicines m
        JOIN pharmacy_prices p ON m.id = p.medicine_id
        WHERE similarity(m.normalized_name, $1) > 0.2
        GROUP BY m.id
        ORDER BY similarity(m.normalized_name, $1) DESC
        LIMIT 20;
        """
        results = await conn.fetch(query, search_term)
        return [dict(r) for r in results]

@app.get("/medicine/{medicine_id}")
async def get_medicine_details(medicine_id: int):
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

@app.get("/categories")
async def get_categories():
    async with db_pool.acquire() as conn:
        return [dict(r) for r in await conn.fetch("SELECT id, name FROM medicine_types ORDER BY name;")]

# Чтобы запустить API, выполните в терминале: uvicorn api:app --reload