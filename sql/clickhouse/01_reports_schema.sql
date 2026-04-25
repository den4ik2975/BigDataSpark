CREATE DATABASE IF NOT EXISTS reports;

DROP TABLE IF EXISTS reports.product_sales_report;
CREATE TABLE reports.product_sales_report (
    product_name Nullable(String),
    product_category Nullable(String),
    total_quantity_sold Int64,
    total_revenue Float64,
    category_revenue Float64,
    avg_rating Nullable(Float64),
    total_reviews Int64,
    sales_rank Int64
) ENGINE = MergeTree
ORDER BY sales_rank;

DROP TABLE IF EXISTS reports.customer_sales_report;
CREATE TABLE reports.customer_sales_report (
    customer_id Int64,
    customer_name Nullable(String),
    email Nullable(String),
    country Nullable(String),
    total_orders Int64,
    total_quantity Int64,
    total_spent Float64,
    avg_order_value Float64,
    country_customer_count Int64,
    spending_rank Int64
) ENGINE = MergeTree
ORDER BY (spending_rank, customer_id);

DROP TABLE IF EXISTS reports.time_sales_report;
CREATE TABLE reports.time_sales_report (
    year_number Int32,
    month_number Int32,
    period_start Date,
    total_orders Int64,
    total_quantity Int64,
    total_revenue Float64,
    avg_order_value Float64,
    prev_month_revenue Nullable(Float64),
    revenue_delta Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (year_number, month_number);

DROP TABLE IF EXISTS reports.store_sales_report;
CREATE TABLE reports.store_sales_report (
    store_id Int64,
    store_name Nullable(String),
    city Nullable(String),
    country Nullable(String),
    total_orders Int64,
    total_quantity Int64,
    total_revenue Float64,
    avg_order_value Float64,
    city_country_revenue Float64,
    revenue_rank Int64
) ENGINE = MergeTree
ORDER BY (revenue_rank, store_id);

DROP TABLE IF EXISTS reports.supplier_sales_report;
CREATE TABLE reports.supplier_sales_report (
    supplier_id Int64,
    supplier_name Nullable(String),
    city Nullable(String),
    country Nullable(String),
    total_products_sold Int64,
    total_revenue Float64,
    supplier_country_revenue Float64,
    avg_product_price Nullable(Float64),
    revenue_rank Int64
) ENGINE = MergeTree
ORDER BY (revenue_rank, supplier_id);

DROP TABLE IF EXISTS reports.product_quality_report;
CREATE TABLE reports.product_quality_report (
    product_id Int64,
    product_name Nullable(String),
    product_category Nullable(String),
    avg_rating Nullable(Float64),
    total_reviews Int64,
    total_quantity_sold Int64,
    total_revenue Float64,
    rating_rank_high Int64,
    rating_rank_low Int64,
    review_rank Int64,
    rating_sales_correlation Nullable(Float64)
) ENGINE = MergeTree
ORDER BY product_id;
