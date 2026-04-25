import os


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/lab2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab")
POSTGRES_DRIVER = "org.postgresql.Driver"

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/reports")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lab")
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def jdbc_options(url, user, password, driver):
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": driver,
    }


def split_sql(sql_text):
    statements = []
    current = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--") or stripped.startswith("\\"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";")
            if statement:
                statements.append(statement)
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def execute_sql_file(spark, path, url, user, password, driver):
    with open(path, "r", encoding="utf-8") as file:
        statements = split_sql(file.read())
    execute_sql_statements(spark, statements, url, user, password, driver)


def execute_sql_statements(spark, statements, url, user, password, driver):
    jvm = spark.sparkContext._gateway.jvm
    driver_class = jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(driver)
    driver_instance = driver_class.newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    connection = driver_instance.connect(url, properties)
    try:
        statement = connection.createStatement()
        try:
            for sql in statements:
                statement.execute(sql)
        finally:
            statement.close()
    finally:
        connection.close()


def read_table(spark, url, user, password, driver, table_name):
    return (
        spark.read.format("jdbc")
        .options(**jdbc_options(url, user, password, driver))
        .option("dbtable", table_name)
        .load()
    )


def write_table(dataframe, url, user, password, driver, table_name):
    (
        dataframe.write.format("jdbc")
        .options(**jdbc_options(url, user, password, driver))
        .option("dbtable", table_name)
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )
