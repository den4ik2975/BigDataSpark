DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product_category CASCADE;
DROP TABLE IF EXISTS dim_pet_category CASCADE;
DROP TABLE IF EXISTS dim_city CASCADE;
DROP TABLE IF EXISTS dim_postal_area CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;

CREATE TABLE dim_country (
    country_id BIGSERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_postal_area (
    postal_area_id BIGSERIAL PRIMARY KEY,
    postal_area_key TEXT NOT NULL UNIQUE,
    country_id BIGINT REFERENCES dim_country(country_id),
    postal_code TEXT
);

CREATE TABLE dim_city (
    city_id BIGSERIAL PRIMARY KEY,
    city_key TEXT NOT NULL UNIQUE,
    city_name TEXT,
    state_name TEXT,
    country_id BIGINT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product_category (
    product_category_id BIGSERIAL PRIMARY KEY,
    product_category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_pet_category (
    pet_category_id BIGSERIAL PRIMARY KEY,
    pet_category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_key TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    postal_area_id BIGINT REFERENCES dim_postal_area(postal_area_id),
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_id BIGSERIAL PRIMARY KEY,
    seller_key TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    postal_area_id BIGINT REFERENCES dim_postal_area(postal_area_id)
);

CREATE TABLE dim_supplier (
    supplier_id BIGSERIAL PRIMARY KEY,
    supplier_key TEXT NOT NULL UNIQUE,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city_id BIGINT REFERENCES dim_city(city_id)
);

CREATE TABLE dim_store (
    store_id BIGSERIAL PRIMARY KEY,
    store_key TEXT NOT NULL UNIQUE,
    store_name TEXT,
    store_location TEXT,
    city_id BIGINT REFERENCES dim_city(city_id),
    phone TEXT,
    email TEXT
);

CREATE TABLE dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    product_key TEXT NOT NULL UNIQUE,
    product_name TEXT,
    product_category_id BIGINT REFERENCES dim_product_category(product_category_id),
    pet_category_id BIGINT REFERENCES dim_pet_category(pet_category_id),
    supplier_id BIGINT REFERENCES dim_supplier(supplier_id),
    product_price NUMERIC(12, 2),
    stock_quantity INTEGER,
    product_weight NUMERIC(10, 2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INTEGER NOT NULL,
    quarter_number INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    day_number INTEGER NOT NULL
);

CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    source_raw_id BIGINT NOT NULL UNIQUE,
    source_sale_id BIGINT,
    source_customer_id BIGINT,
    source_seller_id BIGINT,
    source_product_id BIGINT,
    customer_id BIGINT REFERENCES dim_customer(customer_id),
    seller_id BIGINT REFERENCES dim_seller(seller_id),
    product_id BIGINT REFERENCES dim_product(product_id),
    store_id BIGINT REFERENCES dim_store(store_id),
    date_key INTEGER REFERENCES dim_date(date_key),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(14, 2),
    unit_product_price NUMERIC(12, 2)
);

CREATE INDEX idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product_id ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_customer_id ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_store_id ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_seller_id ON fact_sales(seller_id);
