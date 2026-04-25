from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import (
    POSTGRES_DRIVER,
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    execute_sql_file,
    read_table,
    write_table,
)


STAR_SCHEMA_SQL = "/opt/spark/sql/postgres/03_create_star_schema.sql"


def union_all(dataframes):
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def hash_expr(*expressions):
    parts = [F.coalesce(expr.cast("string"), F.lit("")) for expr in expressions]
    return F.sha2(F.concat_ws("|", *parts), 256)


def with_id(dataframe, id_column, order_columns):
    ordering = [F.col(column).asc_nulls_last() for column in order_columns]
    window = Window.orderBy(*ordering)
    return dataframe.withColumn(id_column, F.row_number().over(window).cast("long")).select(
        id_column, *dataframe.columns
    )


def present(column_name):
    return F.col(column_name).isNotNull() & (F.length(F.trim(F.col(column_name))) > 0)


def parse_source_date(column_name):
    return F.to_date(F.col(column_name), "M/d/yyyy")


def prepare_raw(raw):
    return (
        raw.withColumn("customer_age_value", F.col("customer_age").cast("int"))
        .withColumn("product_price_value", F.col("product_price").cast("decimal(12,2)"))
        .withColumn("product_quantity_value", F.col("product_quantity").cast("int"))
        .withColumn("sale_date_value", parse_source_date("sale_date"))
        .withColumn("sale_quantity_value", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price_value", F.col("sale_total_price").cast("decimal(14,2)"))
        .withColumn("product_weight_value", F.col("product_weight").cast("decimal(10,2)"))
        .withColumn("product_rating_value", F.col("product_rating").cast("decimal(3,2)"))
        .withColumn("product_reviews_value", F.col("product_reviews").cast("int"))
        .withColumn("product_release_date_value", parse_source_date("product_release_date"))
        .withColumn("product_expiry_date_value", parse_source_date("product_expiry_date"))
        .withColumn("customer_postal_area_key", hash_expr(F.col("customer_country"), F.col("customer_postal_code")))
        .withColumn("seller_postal_area_key", hash_expr(F.col("seller_country"), F.col("seller_postal_code")))
        .withColumn("store_city_key", hash_expr(F.col("store_city"), F.col("store_state"), F.col("store_country")))
        .withColumn("supplier_city_key", hash_expr(F.col("supplier_city"), F.lit(""), F.col("supplier_country")))
        .withColumn(
            "customer_key",
            hash_expr(
                F.col("customer_first_name"),
                F.col("customer_last_name"),
                F.col("customer_age"),
                F.col("customer_email"),
                F.col("customer_country"),
                F.col("customer_postal_code"),
                F.col("customer_pet_type"),
                F.col("customer_pet_name"),
                F.col("customer_pet_breed"),
            ),
        )
        .withColumn(
            "seller_key",
            hash_expr(
                F.col("seller_first_name"),
                F.col("seller_last_name"),
                F.col("seller_email"),
                F.col("seller_country"),
                F.col("seller_postal_code"),
            ),
        )
        .withColumn(
            "supplier_key",
            hash_expr(
                F.col("supplier_name"),
                F.col("supplier_contact"),
                F.col("supplier_email"),
                F.col("supplier_phone"),
                F.col("supplier_address"),
                F.col("supplier_city"),
                F.col("supplier_country"),
            ),
        )
        .withColumn(
            "store_key",
            hash_expr(
                F.col("store_name"),
                F.col("store_location"),
                F.col("store_city"),
                F.col("store_state"),
                F.col("store_country"),
                F.col("store_phone"),
                F.col("store_email"),
            ),
        )
        .withColumn(
            "product_key",
            hash_expr(
                F.col("product_name"),
                F.col("product_category"),
                F.col("product_price"),
                F.col("product_quantity"),
                F.col("pet_category"),
                F.col("product_weight"),
                F.col("product_color"),
                F.col("product_size"),
                F.col("product_brand"),
                F.col("product_material"),
                F.col("product_description"),
                F.col("product_rating"),
                F.col("product_reviews"),
                F.col("product_release_date"),
                F.col("product_expiry_date"),
                F.col("supplier_name"),
                F.col("supplier_email"),
            ),
        )
    )


def build_dimensions(prepared):
    countries = with_id(
        union_all(
            [
                prepared.select(F.col("customer_country").alias("country_name")),
                prepared.select(F.col("seller_country").alias("country_name")),
                prepared.select(F.col("store_country").alias("country_name")),
                prepared.select(F.col("supplier_country").alias("country_name")),
            ]
        )
        .where(present("country_name"))
        .distinct(),
        "country_id",
        ["country_name"],
    )

    postal_sources = (
        union_all(
            [
                prepared.select(
                    F.col("customer_postal_area_key").alias("postal_area_key"),
                    F.col("customer_country").alias("country_name"),
                    F.col("customer_postal_code").alias("postal_code"),
                ),
                prepared.select(
                    F.col("seller_postal_area_key").alias("postal_area_key"),
                    F.col("seller_country").alias("country_name"),
                    F.col("seller_postal_code").alias("postal_code"),
                ),
            ]
        )
        .where(present("country_name"))
        .distinct()
    )
    postal_areas = with_id(
        postal_sources.join(countries, "country_name", "left")
        .select("postal_area_key", "country_id", "postal_code")
        .distinct(),
        "postal_area_id",
        ["postal_area_key"],
    )

    city_sources = (
        union_all(
            [
                prepared.select(
                    F.col("store_city_key").alias("city_key"),
                    F.col("store_city").alias("city_name"),
                    F.col("store_state").alias("state_name"),
                    F.col("store_country").alias("country_name"),
                ),
                prepared.select(
                    F.col("supplier_city_key").alias("city_key"),
                    F.col("supplier_city").alias("city_name"),
                    F.lit(None).cast("string").alias("state_name"),
                    F.col("supplier_country").alias("country_name"),
                ),
            ]
        )
        .where(present("country_name"))
        .distinct()
    )
    cities = with_id(
        city_sources.join(countries, "country_name", "left")
        .select("city_key", "city_name", "state_name", "country_id")
        .distinct(),
        "city_id",
        ["city_key"],
    )

    product_categories = with_id(
        prepared.select(F.col("product_category").alias("product_category_name"))
        .where(present("product_category_name"))
        .distinct(),
        "product_category_id",
        ["product_category_name"],
    )

    pet_categories = with_id(
        prepared.select(F.col("pet_category").alias("pet_category_name")).where(present("pet_category_name")).distinct(),
        "pet_category_id",
        ["pet_category_name"],
    )

    customers = with_id(
        prepared.select(
            "customer_key",
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age_value").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_postal_area_key").alias("postal_area_key"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
        )
        .dropDuplicates(["customer_key"])
        .join(postal_areas.select("postal_area_key", "postal_area_id"), "postal_area_key", "left")
        .drop("postal_area_key"),
        "customer_id",
        ["customer_key"],
    )

    sellers = with_id(
        prepared.select(
            "seller_key",
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_postal_area_key").alias("postal_area_key"),
        )
        .dropDuplicates(["seller_key"])
        .join(postal_areas.select("postal_area_key", "postal_area_id"), "postal_area_key", "left")
        .drop("postal_area_key"),
        "seller_id",
        ["seller_key"],
    )

    suppliers = with_id(
        prepared.select(
            "supplier_key",
            F.col("supplier_name").alias("supplier_name"),
            F.col("supplier_contact").alias("contact_name"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city_key").alias("city_key"),
        )
        .dropDuplicates(["supplier_key"])
        .join(cities.select("city_key", "city_id"), "city_key", "left")
        .drop("city_key"),
        "supplier_id",
        ["supplier_key"],
    )

    stores = with_id(
        prepared.select(
            "store_key",
            F.col("store_name").alias("store_name"),
            F.col("store_location").alias("store_location"),
            F.col("store_city_key").alias("city_key"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .dropDuplicates(["store_key"])
        .join(cities.select("city_key", "city_id"), "city_key", "left")
        .drop("city_key"),
        "store_id",
        ["store_key"],
    )

    products = with_id(
        prepared.select(
            "product_key",
            F.col("product_name").alias("product_name"),
            F.col("product_category").alias("product_category_name"),
            F.col("pet_category").alias("pet_category_name"),
            "supplier_key",
            F.col("product_price_value").alias("product_price"),
            F.col("product_quantity_value").alias("stock_quantity"),
            F.col("product_weight_value").alias("product_weight"),
            F.col("product_color").alias("product_color"),
            F.col("product_size").alias("product_size"),
            F.col("product_brand").alias("product_brand"),
            F.col("product_material").alias("product_material"),
            F.col("product_description").alias("product_description"),
            F.col("product_rating_value").alias("product_rating"),
            F.col("product_reviews_value").alias("product_reviews"),
            F.col("product_release_date_value").alias("product_release_date"),
            F.col("product_expiry_date_value").alias("product_expiry_date"),
        )
        .dropDuplicates(["product_key"])
        .join(product_categories, "product_category_name", "left")
        .join(pet_categories, "pet_category_name", "left")
        .join(suppliers.select("supplier_key", "supplier_id"), "supplier_key", "left")
        .select(
            "product_key",
            "product_name",
            "product_category_id",
            "pet_category_id",
            "supplier_id",
            "product_price",
            "stock_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        ),
        "product_id",
        ["product_key"],
    )

    dates = (
        prepared.select(F.col("sale_date_value").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year_number", F.year("full_date").cast("int"))
        .withColumn("quarter_number", F.quarter("full_date").cast("int"))
        .withColumn("month_number", F.month("full_date").cast("int"))
        .withColumn("day_number", F.dayofmonth("full_date").cast("int"))
        .select("date_key", "full_date", "year_number", "quarter_number", "month_number", "day_number")
    )

    return {
        "dim_country": countries,
        "dim_postal_area": postal_areas,
        "dim_city": cities,
        "dim_product_category": product_categories,
        "dim_pet_category": pet_categories,
        "dim_customer": customers,
        "dim_seller": sellers,
        "dim_supplier": suppliers,
        "dim_store": stores,
        "dim_product": products,
        "dim_date": dates,
    }


def build_fact(prepared, dimensions):
    return (
        prepared.select(
            F.col("raw_id").cast("long").alias("source_raw_id"),
            F.col("id").cast("long").alias("source_sale_id"),
            F.col("sale_customer_id").cast("long").alias("source_customer_id"),
            F.col("sale_seller_id").cast("long").alias("source_seller_id"),
            F.col("sale_product_id").cast("long").alias("source_product_id"),
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "sale_date_value",
            F.col("sale_quantity_value").alias("sale_quantity"),
            F.col("sale_total_price_value").alias("sale_total_price"),
            F.col("product_price_value").alias("unit_product_price"),
        )
        .join(dimensions["dim_customer"].select("customer_key", "customer_id"), "customer_key")
        .join(dimensions["dim_seller"].select("seller_key", "seller_id"), "seller_key")
        .join(dimensions["dim_product"].select("product_key", "product_id"), "product_key")
        .join(dimensions["dim_store"].select("store_key", "store_id"), "store_key")
        .withColumn("date_key", F.date_format("sale_date_value", "yyyyMMdd").cast("int"))
        .select(
            "source_raw_id",
            "source_sale_id",
            "source_customer_id",
            "source_seller_id",
            "source_product_id",
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            "date_key",
            "sale_quantity",
            "sale_total_price",
            "unit_product_price",
        )
    )


def main():
    spark = SparkSession.builder.appName("pet-sales-postgres-star-etl").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    execute_sql_file(spark, STAR_SCHEMA_SQL, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER)

    raw = read_table(spark, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.mock_data")
    prepared = prepare_raw(raw)
    dimensions = build_dimensions(prepared)
    fact_sales = build_fact(prepared, dimensions)

    write_order = [
        "dim_country",
        "dim_postal_area",
        "dim_city",
        "dim_product_category",
        "dim_pet_category",
        "dim_customer",
        "dim_seller",
        "dim_supplier",
        "dim_store",
        "dim_product",
        "dim_date",
    ]

    for table_name in write_order:
        dataframe = dimensions[table_name]
        write_table(dataframe, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, f"public.{table_name}")
        print(f"{table_name}: {dataframe.count()} rows")

    write_table(fact_sales, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER, "public.fact_sales")
    print(f"fact_sales: {fact_sales.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
