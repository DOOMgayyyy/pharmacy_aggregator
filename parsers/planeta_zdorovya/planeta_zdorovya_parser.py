# parsers/planeta_zdorovya_parser.py

import json
import re
import asyncpg
from typing import Dict, Any
import asyncio
from ..base_parser import BaseParser, light_normalize

class PlanetaZdorovyaParser(BaseParser):
    """
    Parser for the 'Planeta Zdorovya' pharmacy.
    This parser works with a local JSON file to populate the database.
    """

    def __init__(self, db_pool: asyncpg.Pool):
        super().__init__(session=None, db_pool=db_pool)
        self.pharmacy_id = 2

    async def _get_or_create_medicine(self, conn: asyncpg.Connection, product_name: str) -> int:
        """
        Retrieves the ID of a medicine if it exists, or creates it if it doesn't.
        Returns the medicine ID.
        """
        normalized_name = light_normalize(product_name)
        
        # Try to find an exact match first
        medicine_id = await conn.fetchval(
            "SELECT id FROM medicines WHERE name = $1", normalized_name
        )

        if medicine_id:
            return medicine_id
        
        # If no exact match, create it
        medicine_id = await conn.fetchval(
            """
            INSERT INTO medicines (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE
            SET name = EXCLUDED.name
            RETURNING id
            """,
            normalized_name
        )
        return medicine_id

    async def populate_medicines_from_json(self, file_path: str):
        """
        Parses the JSON file and populates the 'medicines' table.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            products = json.load(f)

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                for i, product in enumerate(products):
                    product_name = product.get("title")
                    if product_name:
                        await self._get_or_create_medicine(conn, product_name)
                    
                    # Add a small delay every 100 products to prevent overloading
                    if i % 100 == 0:
                        await asyncio.sleep(0.1)
        
        print(f"✅ Medicine population from {file_path} is complete.")

    async def parse_prices_from_json(self, file_path: str):
        """
        Parses the JSON file to populate prices for 'Planeta Zdorovya'.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        price_data_for_json = []
        processed_count = 0

        async with self.db_pool.acquire() as conn:
            for i, product in enumerate(products):
                product_name = product.get("title")
                price_str = product.get("price")

                if not product_name or not price_str:
                    continue

                # Clean the price string to get a number
                price_match = re.search(r'(\d+)', price_str)
                if not price_match:
                    continue
                
                price = float(price_match.group(1))
                normalized_name = light_normalize(product_name)
                
                # First try exact match
                medicine_id = await conn.fetchval(
                    "SELECT id FROM medicines WHERE name = $1", normalized_name
                )

                # If no exact match, try similarity search
                if not medicine_id:
                    medicine_id = await conn.fetchval(
                        """
                        SELECT id FROM medicines 
                        ORDER BY similarity(name, $1) DESC 
                        LIMIT 1
                        """,
                        normalized_name
                    )

                if medicine_id:
                    # Insert or update the price in the database
                    await conn.execute(
                        """
                        INSERT INTO pharmacy_prices (pharmacy_id, medicine_id, price)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (pharmacy_id, medicine_id)
                        DO UPDATE SET price = EXCLUDED.price, last_updated = NOW()
                        """,
                        self.pharmacy_id, medicine_id, price
                    )
                    
                    # Append data for JSON file output
                    price_data_for_json.append({
                        "pharmacy_id": self.pharmacy_id,
                        "medicine_id": medicine_id,
                        "price": price
                    })
                    
                    processed_count += 1
                    
                    # Add a small delay every 50 products to prevent overloading
                    if processed_count % 50 == 0:
                        await asyncio.sleep(0.1)
                        print(f"Processed {processed_count} products...")

        # Save the collected price data to a JSON file
        output_filename = "planeta_zdorovya_prices.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(price_data_for_json, f, indent=4, ensure_ascii=False)

        print(f"✅ Price parsing is complete. Processed {processed_count} products. Data saved to {output_filename}.")