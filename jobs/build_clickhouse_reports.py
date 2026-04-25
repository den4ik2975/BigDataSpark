from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import (
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    POSTGRES_DRIVER,
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    execute_sql_file,
    read_table,
    write_table,
)


CLICKHOUSE_SCHEMA_SQL = "/opt/spark/sql/clickhouse/01_reports_schema.sql"


def money_sum(column_name):
    return F.coalesce(F.sum(F.col(column_name)).cast("double"), F.lit(0.0))


def quantity_sum(column_name):
    return F.coalesce(F.sum(F.col(column_name)).cast("long"), F.lit(0))


def load_sales_mart(spark):
    fact = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.fact_sales")
    products = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_product")
    product_categories = read_table(
        spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_product_category"
    )
    customers = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_customer")
    postal_areas = read_table(
        spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_postal_area"
    )
    countries = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_country")
    stores = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_store")
    cities = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_city")
    suppliers = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_supplier")
    dates = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.dim_date")

    product_dim = (
        products.join(product_categories, "product_category_id", "left")
        .select(
            "product_id",
            "supplier_id",
            "product_name",
            F.col("product_category_name").alias("product_category"),
            F.col("product_price").cast("double").alias("product_price"),
            F.col("product_rating").cast("double").alias("product_rating"),
            F.col("product_reviews").cast("long").alias("product_reviews"),
        )
    )

    customer_dim = (
        customers.join(postal_areas, "postal_area_id", "left")
        .join(countries, "country_id", "left")
        .select(
            "customer_id",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
            "email",
            F.col("country_name").alias("customer_country"),
        )
    )

    store_dim = (
        stores.join(cities, "city_id", "left")
        .join(countries, "country_id", "left")
        .select(
            "store_id",
            "store_name",
            F.col("city_name").alias("store_city"),
            F.col("country_name").alias("store_country"),
        )
    )

    supplier_dim = (
        suppliers.join(cities, "city_id", "left")
        .join(countries, "country_id", "left")
        .select(
            "supplier_id",
            "supplier_name",
            F.col("city_name").alias("supplier_city"),
            F.col("country_name").alias("supplier_country"),
        )
    )

    return (
        fact.join(product_dim, "product_id", "left")
        .join(customer_dim, "customer_id", "left")
        .join(store_dim, "store_id", "left")
        .join(supplier_dim, "supplier_id", "left")
        .join(dates, "date_key", "left")
    )


def build_product_report(sales):
    ranked = Window.orderBy(F.col("total_quantity_sold").desc(), F.col("total_revenue").desc())
    category_window = Window.partitionBy("product_category")
    sales_metrics = sales.groupBy("product_name", "product_category").agg(
        quantity_sum("sale_quantity").alias("total_quantity_sold"),
        money_sum("sale_total_price").alias("total_revenue"),
    )
    product_metrics = (
        sales.select("product_id", "product_name", "product_category", "product_rating", "product_reviews")
        .dropDuplicates(["product_id"])
        .groupBy("product_name", "product_category")
        .agg(
            F.avg("product_rating").cast("double").alias("avg_rating"),
            F.coalesce(F.sum("product_reviews").cast("long"), F.lit(0)).alias("total_reviews"),
        )
    )
    return (
        sales_metrics.join(product_metrics, ["product_name", "product_category"], "left")
        .withColumn("category_revenue", F.sum("total_revenue").over(category_window).cast("double"))
        .withColumn("sales_rank", F.row_number().over(ranked).cast("long"))
        .select(
            "product_name",
            "product_category",
            "total_quantity_sold",
            "total_revenue",
            "category_revenue",
            "avg_rating",
            "total_reviews",
            "sales_rank",
        )
    )


def build_customer_report(sales):
    country_counts = (
        sales.select("customer_id", "customer_country")
        .dropDuplicates(["customer_id"])
        .groupBy("customer_country")
        .agg(F.count("customer_id").cast("long").alias("country_customer_count"))
    )
    ranked = Window.orderBy(F.col("total_spent").desc(), F.col("customer_id").asc())
    return (
        sales.groupBy("customer_id", "customer_name", "email", "customer_country")
        .agg(
            F.count("source_raw_id").cast("long").alias("total_orders"),
            quantity_sum("sale_quantity").alias("total_quantity"),
            money_sum("sale_total_price").alias("total_spent"),
        )
        .join(country_counts, "customer_country", "left")
        .withColumn("avg_order_value", (F.col("total_spent") / F.col("total_orders")).cast("double"))
        .withColumn("spending_rank", F.row_number().over(ranked).cast("long"))
        .select(
            "customer_id",
            "customer_name",
            "email",
            F.col("customer_country").alias("country"),
            "total_orders",
            "total_quantity",
            "total_spent",
            "avg_order_value",
            F.coalesce(F.col("country_customer_count"), F.lit(0)).cast("long").alias("country_customer_count"),
            "spending_rank",
        )
    )


def build_time_report(sales):
    period_window = Window.orderBy("period_start")
    monthly = (
        sales.where(F.col("full_date").isNotNull())
        .withColumn("period_start", F.trunc("full_date", "MM"))
        .groupBy("year_number", "month_number", "period_start")
        .agg(
            F.count("source_raw_id").cast("long").alias("total_orders"),
            quantity_sum("sale_quantity").alias("total_quantity"),
            money_sum("sale_total_price").alias("total_revenue"),
        )
        .withColumn("avg_order_value", (F.col("total_revenue") / F.col("total_orders")).cast("double"))
        .withColumn("prev_month_revenue", F.lag("total_revenue").over(period_window).cast("double"))
        .withColumn("revenue_delta", (F.col("total_revenue") - F.col("prev_month_revenue")).cast("double"))
    )
    return monthly.select(
        F.col("year_number").cast("int").alias("year_number"),
        F.col("month_number").cast("int").alias("month_number"),
        "period_start",
        "total_orders",
        "total_quantity",
        "total_revenue",
        "avg_order_value",
        "prev_month_revenue",
        "revenue_delta",
    )


def build_store_report(sales):
    city_revenue = (
        sales.groupBy("store_city", "store_country")
        .agg(money_sum("sale_total_price").alias("city_country_revenue"))
    )
    ranked = Window.orderBy(F.col("total_revenue").desc(), F.col("store_id").asc())
    return (
        sales.groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.count("source_raw_id").cast("long").alias("total_orders"),
            quantity_sum("sale_quantity").alias("total_quantity"),
            money_sum("sale_total_price").alias("total_revenue"),
        )
        .join(city_revenue, ["store_city", "store_country"], "left")
        .withColumn("avg_order_value", (F.col("total_revenue") / F.col("total_orders")).cast("double"))
        .withColumn("revenue_rank", F.row_number().over(ranked).cast("long"))
        .select(
            "store_id",
            "store_name",
            F.col("store_city").alias("city"),
            F.col("store_country").alias("country"),
            "total_orders",
            "total_quantity",
            "total_revenue",
            "avg_order_value",
            F.coalesce(F.col("city_country_revenue"), F.col("total_revenue")).cast("double").alias(
                "city_country_revenue"
            ),
            "revenue_rank",
        )
    )


def build_supplier_report(sales):
    ranked = Window.orderBy(F.col("total_revenue").desc(), F.col("supplier_id").asc())
    country_window = Window.partitionBy("supplier_country")
    return (
        sales.groupBy("supplier_id", "supplier_name", "supplier_city", "supplier_country")
        .agg(
            quantity_sum("sale_quantity").alias("total_products_sold"),
            money_sum("sale_total_price").alias("total_revenue"),
            F.avg("product_price").cast("double").alias("avg_product_price"),
        )
        .withColumn("supplier_country_revenue", F.sum("total_revenue").over(country_window).cast("double"))
        .withColumn("revenue_rank", F.row_number().over(ranked).cast("long"))
        .select(
            "supplier_id",
            "supplier_name",
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
            "total_products_sold",
            "total_revenue",
            "supplier_country_revenue",
            "avg_product_price",
            "revenue_rank",
        )
    )


def build_quality_report(sales):
    quality = (
        sales.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.avg("product_rating").cast("double").alias("avg_rating"),
            F.coalesce(F.max("product_reviews").cast("long"), F.lit(0)).alias("total_reviews"),
            quantity_sum("sale_quantity").alias("total_quantity_sold"),
            money_sum("sale_total_price").alias("total_revenue"),
        )
    )
    corr_value = quality.agg(F.corr("avg_rating", "total_quantity_sold").alias("corr")).collect()[0]["corr"]
    ranked_high = Window.orderBy(F.col("avg_rating").desc_nulls_last(), F.col("product_id").asc())
    ranked_low = Window.orderBy(F.col("avg_rating").asc_nulls_last(), F.col("product_id").asc())
    ranked_reviews = Window.orderBy(F.col("total_reviews").desc(), F.col("product_id").asc())
    return (
        quality.withColumn("rating_rank_high", F.row_number().over(ranked_high).cast("long"))
        .withColumn("rating_rank_low", F.row_number().over(ranked_low).cast("long"))
        .withColumn("review_rank", F.row_number().over(ranked_reviews).cast("long"))
        .withColumn("rating_sales_correlation", F.lit(corr_value).cast("double"))
        .select(
            "product_id",
            "product_name",
            "product_category",
            "avg_rating",
            "total_reviews",
            "total_quantity_sold",
            "total_revenue",
            "rating_rank_high",
            "rating_rank_low",
            "review_rank",
            "rating_sales_correlation",
        )
    )


def main():
    spark = SparkSession.builder.appName("pet-sales-clickhouse-reports").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    execute_sql_file(
        spark,
        CLICKHOUSE_SCHEMA_SQL,
        CLICKHOUSE_URL,
        CLICKHOUSE_USER,
        CLICKHOUSE_PASSWORD,
        CLICKHOUSE_DRIVER,
    )

    sales = load_sales_mart(spark)
    reports = {
        "reports.product_sales_report": build_product_report(sales),
        "reports.customer_sales_report": build_customer_report(sales),
        "reports.time_sales_report": build_time_report(sales),
        "reports.store_sales_report": build_store_report(sales),
        "reports.supplier_sales_report": build_supplier_report(sales),
        "reports.product_quality_report": build_quality_report(sales),
    }

    for table_name, dataframe in reports.items():
        write_table(dataframe, CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DRIVER, table_name)
        print(f"{table_name}: {dataframe.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
