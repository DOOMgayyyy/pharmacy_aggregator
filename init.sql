-- & "C:\Program Files\PostgreSQL\17\bin\psql.exe" -U user_farm -d db_farm -f init.sql
-- Включаем модуль для "нечеткого" сравнения строк. Выполнить один раз.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Таблица для хранения типов/категорий лекарств (создается из "Госаптеки")
CREATE TABLE medicine_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- ГЛАВНАЯ ТАБЛИЦА: Эталонный каталог всех лекарств
CREATE TABLE medicines (
    id SERIAL PRIMARY KEY,
    -- "Красивое" имя для отображения пользователю
    name VARCHAR(255) NOT NULL UNIQUE,
    -- "Техническое" имя для поиска и сопоставления (например, "нурофен форте")
    normalized_name VARCHAR(255),
    description TEXT,
    image_url VARCHAR(255),
    type_id INTEGER REFERENCES medicine_types(id) ON DELETE SET NULL
);

-- Создаем супер-быстрый индекс для поиска по техническому имени
CREATE INDEX idx_medicines_normalized_name_trgm ON medicines USING gin (normalized_name gin_trgm_ops);

-- Справочник аптек, которые мы парсим
CREATE TABLE pharmacies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    -- Базовый URL сайта используется как уникальный идентификатор
    address VARCHAR(255) UNIQUE
);

-- Таблица с ценами от разных аптек, ссылающаяся на эталонный каталог
CREATE TABLE pharmacy_prices (
    pharmacy_id INTEGER NOT NULL REFERENCES pharmacies(id) ON DELETE CASCADE,
    medicine_id INTEGER NOT NULL REFERENCES medicines(id) ON DELETE CASCADE,
    price NUMERIC(10, 2) NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    -- Уникальная связка: одно лекарство - одна цена в одной аптеке
    PRIMARY KEY (pharmacy_id, medicine_id)
);