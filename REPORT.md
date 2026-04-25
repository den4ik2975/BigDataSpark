# Отчет по лабораторной работе N2

## Тема

ETL-пайплайн на Apache Spark для преобразования исходных данных в модель снежинка/звезда в PostgreSQL и построения аналитических витрин в ClickHouse.

## Цель работы

Реализовать Spark ETL, который:

1. читает исходную плоскую таблицу `mock_data` из PostgreSQL;
2. строит модель снежинка/звезда в PostgreSQL;
3. создает 6 аналитических отчетов в ClickHouse.

## Исходные данные

В папке `исходные данные` находится 10 CSV-файлов по 1000 строк. При старте PostgreSQL данные автоматически загружаются в таблицу `mock_data`. Общее количество строк: 10000.

## Используемые технологии

- Docker Compose;
- PostgreSQL 16;
- Apache Spark 3.5.1;
- PySpark;
- ClickHouse 24.8;
- JDBC-драйверы PostgreSQL и ClickHouse.

## Структура решения

- `docker-compose.yml` - запуск PostgreSQL, ClickHouse и Spark.
- `sql/postgres/01_create_raw.sql` - создание сырой таблицы.
- `sql/postgres/02_load_raw.sql` - загрузка исходных CSV.
- `sql/postgres/03_create_star_schema.sql` - DDL модели снежинка/звезда.
- `sql/clickhouse/01_reports_schema.sql` - DDL таблиц отчетов в ClickHouse.
- `jobs/common.py` - общие JDBC-хелперы.
- `jobs/etl_to_postgres_star.py` - Spark-джоба преобразования `mock_data` в снежинку/звезду PostgreSQL.
- `jobs/build_clickhouse_reports.py` - Spark-джоба построения отчетов в ClickHouse.

## Модель PostgreSQL

Фактовая таблица:

- `fact_sales` - продажи, количество, выручка, цена единицы, ссылки на измерения.

Измерения:

- `dim_customer`;
- `dim_seller`;
- `dim_product`;
- `dim_supplier`;
- `dim_store`;
- `dim_date`;
- `dim_country`;
- `dim_city`;
- `dim_postal_area`;
- `dim_product_category`;
- `dim_pet_category`.

## Отчеты ClickHouse

Созданы 6 таблиц-витрин:

1. `product_sales_report` - продажи по продуктам: количество, выручка продукта, выручка категории, рейтинг, отзывы, ранг продаж.
2. `customer_sales_report` - продажи по клиентам: сумма покупок, средний чек, страна, ранг клиента.
3. `time_sales_report` - продажи по времени: месячные тренды, выручка, средний чек, изменение к предыдущему месяцу.
4. `store_sales_report` - продажи по магазинам: выручка, средний чек, город и страна.
5. `supplier_sales_report` - продажи по поставщикам: выручка поставщика, выручка страны поставщика, количество проданных товаров, средняя цена.
6. `product_quality_report` - качество продукции: рейтинги, отзывы, продажи, корреляция рейтинга и объема продаж.

## Соответствие требованиям

| Требование README | Реализация |
| --- | --- |
| 10 исходных CSV-файлов по 1000 строк | Папка `исходные данные`, загрузка в `sql/postgres/02_load_raw.sql` |
| PostgreSQL, Spark и ClickHouse в Docker Compose | `docker-compose.yml` |
| Заполненная таблица `mock_data` в PostgreSQL | `sql/postgres/01_create_raw.sql`, `sql/postgres/02_load_raw.sql` |
| Spark ETL из исходной модели в снежинку/звезду PostgreSQL | `jobs/etl_to_postgres_star.py` |
| Spark ETL из снежинки/звезды в ClickHouse | `jobs/build_clickhouse_reports.py` |
| 6 отдельных таблиц отчетов в ClickHouse | `sql/clickhouse/01_reports_schema.sql` |
| Инструкция запуска Spark-джоб | README и раздел "Запуск" этого отчета |

## Покрытие аналитических требований

| Аналитическое требование | Таблица / поле |
| --- | --- |
| Топ-10 самых продаваемых продуктов | `product_sales_report.sales_rank <= 10` |
| Общая выручка по категориям продуктов | `product_sales_report.product_category`, `category_revenue` |
| Средний рейтинг и количество отзывов для каждого продукта | `product_sales_report.avg_rating`, `total_reviews` |
| Топ-10 клиентов по сумме покупок | `customer_sales_report.spending_rank <= 10` |
| Распределение клиентов по странам | `customer_sales_report.country`, `country_customer_count` |
| Средний чек для каждого клиента | `customer_sales_report.avg_order_value` |
| Месячные и годовые тренды продаж | `time_sales_report.year_number`, `month_number`, `total_revenue` |
| Сравнение выручки за разные периоды | `time_sales_report.prev_month_revenue`, `revenue_delta` |
| Средний размер заказа по месяцам | `time_sales_report.avg_order_value` |
| Топ-5 магазинов по выручке | `store_sales_report.revenue_rank <= 5` |
| Распределение продаж по городам и странам | `store_sales_report.city`, `country`, `city_country_revenue` |
| Средний чек для каждого магазина | `store_sales_report.avg_order_value` |
| Топ-5 поставщиков по выручке | `supplier_sales_report.revenue_rank <= 5` |
| Средняя цена товаров от поставщика | `supplier_sales_report.avg_product_price` |
| Распределение продаж по странам поставщиков | `supplier_sales_report.country`, `supplier_country_revenue` |
| Продукты с максимальным и минимальным рейтингом | `product_quality_report.rating_rank_high`, `rating_rank_low` |
| Корреляция рейтинга и объема продаж | `product_quality_report.rating_sales_correlation` |
| Продукты с наибольшим количеством отзывов | `product_quality_report.review_rank` |

## Запуск

Для чистого запуска:

```bash
docker compose down -v
docker compose up -d
```

Построение модели снежинка/звезда в PostgreSQL:

```bash
docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/etl_to_postgres_star.py
```

Построение отчетов в ClickHouse:

```bash
docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/build_clickhouse_reports.py
```

## Параметры подключения

PostgreSQL:

- host: `localhost`
- port: `5433`
- database: `lab2`
- user: `lab`
- password: `lab`

ClickHouse:

- host: `localhost`
- HTTP port: `8123`
- native port: `9000`
- database: `reports`
- user: `default`
- password: `lab`

## Проверка результата

Проверка PostgreSQL:

```bash
docker compose exec -T postgres psql -U lab -d lab2 -c "SELECT COUNT(*) FROM mock_data;"
docker compose exec -T postgres psql -U lab -d lab2 -c "SELECT COUNT(*) FROM fact_sales;"
```

Полученные результаты PostgreSQL:

| Таблица | Количество строк |
| --- | ---: |
| `mock_data` | 10000 |
| `fact_sales` | 10000 |
| `dim_country` | 230 |
| `dim_product` | 10000 |
| `dim_date` | 364 |

Проверка ClickHouse:

```bash
docker compose exec -T clickhouse clickhouse-client --password lab \
  --query "SELECT table, total_rows FROM system.tables WHERE database='reports' AND table LIKE '%_report' ORDER BY table"
```

Полученные результаты ClickHouse:

| Отчет | Количество строк |
| --- | ---: |
| `customer_sales_report` | 10000 |
| `product_quality_report` | 10000 |
| `product_sales_report` | 9 |
| `store_sales_report` | 10000 |
| `supplier_sales_report` | 10000 |
| `time_sales_report` | 12 |

В `product_sales_report` получилось 9 строк, потому что в исходных данных 9 уникальных пар `product_name` и `product_category`; требование топ-10 покрывается полем `sales_rank` для всех доступных продуктов.

## Вывод

Spark ETL успешно преобразует исходные данные из PostgreSQL в аналитическую модель снежинка/звезда и формирует 6 обязательных витрин в ClickHouse. Полученные отчеты позволяют анализировать продажи по продуктам, клиентам, времени, магазинам, поставщикам и качеству продукции.
