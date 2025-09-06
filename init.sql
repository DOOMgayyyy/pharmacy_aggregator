-- Enable the trigram extension for similarity searches
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Table for hierarchical categories
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_id INTEGER REFERENCES categories(id) ON DELETE CASCADE, -- Link to parent category
    UNIQUE (name, parent_id) -- A category name must be unique within its parent
);

-- Main table for the master product catalog
CREATE TABLE medicines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    image_url VARCHAR(255),
    category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL
);

-- Create a GIN index on the 'name' column for fast similarity searches
CREATE INDEX idx_medicines_name_trgm ON medicines USING gin (name gin_trgm_ops);

-- Table for pharmacy information
CREATE TABLE pharmacies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) UNIQUE -- Base URL as a unique identifier
);

-- Table for storing prices from different pharmacies
CREATE TABLE pharmacy_prices (
    pharmacy_id INTEGER NOT NULL REFERENCES pharmacies(id) ON DELETE CASCADE,
    medicine_id INTEGER NOT NULL REFERENCES medicines(id) ON DELETE CASCADE,
    price NUMERIC(10, 2) NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (pharmacy_id, medicine_id)
);