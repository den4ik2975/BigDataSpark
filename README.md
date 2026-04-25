# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

Что необходимо сделать? 

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
Цель: Анализ выручки, количества продаж и популярности продуктов.
 - Топ-10 самых продаваемых продуктов.
 - Общая выручка по категориям продуктов.
 - Средний рейтинг и количество отзывов для каждого продукта.
2. Витрина продаж по клиентам
Цель: Анализ покупательского поведения и сегментация клиентов.
 - Топ-10 клиентов с наибольшей общей суммой покупок.
 - Распределение клиентов по странам.
 - Средний чек для каждого клиента.
3. Витрина продаж по времени
Цель: Анализ сезонности и трендов продаж.
 - Месячные и годовые тренды продаж.
 - Сравнение выручки за разные периоды.
 - Средний размер заказа по месяцам.
4. Витрина продаж по магазинам
Цель: Анализ эффективности магазинов.
 - Топ-5 магазинов с наибольшей выручкой.
 - Распределение продаж по городам и странам.
 - Средний чек для каждого магазина.
5. Витрина продаж по поставщикам
Цель: Анализ эффективности поставщиков.
 - Топ-5 поставщиков с наибольшей выручкой.
 - Средняя цена товаров от каждого поставщика.
 - Распределение продаж по странам поставщиков.
6. Витрина качества продукции
Цель: Анализ отзывов и рейтингов товаров.
 - Продукты с наивысшим и наименьшим рейтингом.
 - Корреляция между рейтингом и объемом продаж.
 - Продукты с наибольшим количеством отзывов.

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)**
2. Cassandra (опционально, если будет реализация, то это бонус)
3. Neo4J (опционально, если будет реализация, то это бонус)
4. MongoDB (опционально, если будет реализация, то это бонус)
5. Valkey (опционально, если будет реализация, то это бонус)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Spark (рекомендую установку через Docker. Для удобства написания кода на Python можно запустить вместе со JupyterNotebook. Для Java - подключить volume и собрать образ Docker, который будет запускать команду spark-submit с java jar-файлом при старте контейнера, сам jar файл собирается отдельно и кладется в подключенный volume)
5. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
6. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
7. Анализируете исходные данные с помощью запросов.
8. Выявляете сущности фактов и измерений.
9. Реализуете приложение на Spark, которое по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель снежинку/звезда в PostgreSQL. (Убедитесь в коннективности Spark и PostgreSQL, настройте сеть между Spark и PostgreSQL, если используете Docker).
10. Устанавливаете ClickHouse (рекомендую установку через Docker. Убедитесь в коннективности Spark и Clickhouse, настройте сеть между Spark и ClickHouse). **(обязательно)**
11. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в ClickHouse. **(обязательно)**
12. Устанавливаете Cassandra (рекомендую установку через Docker. Убедитесь в коннективности Spark и Cassandra, настройте сеть между Spark и Cassandra). (опционально)
13. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в Cassandra. (опционально)
14. Устанавливаете Neo4j (рекомендую установку через Docker. Убедитесь в коннективности Spark и Neo4j, настройте сеть между Spark и Neo4j). (опционально)
15. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных сущностей в Neo4j. (опционально)
16. Устанавливаете MongoDB (рекомендую установку через Docker. Убедитесь в коннективности Spark и MongoDB, настройте сеть между Spark и MongoDB). (опционально)
17. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных коллекций в MongoDB. (опционально)
18. Устанавливаете Valkey (рекомендую установку через Docker. Убедитесь в коннективности Spark и Valkey, настройте сеть между Spark и Valkey). (опционально)
19. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных записей в Valkey. (опционально)
20. Проверяете отчеты в каждой базе данных средствами языка самой БД (ClickHouse - SQL (DBeaver), Cassandra - CQL (DBeaver), Neo4J - Cipher (DBeaver), MongoDB - MQL (Compass), Valkey - redis-cli).
21. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Spark, ClickHouse **(обязательно)**, Cassandra (опционально), Neo4j (опционально), MongoDB (опционально), Valkey (опционально) и заполненными данными в PostgreSQL из файлов mock_data(*).csv.
3. Инструкция, как запускать Spark-джобы для проверки лабораторной работы.
4. Код Apache Spark трансформации данных из исходной модели в снежинку/звезду в PostgreSQL.
5. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в ClickHouse.
6. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Cassandra.
7. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Neo4j.
8. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в MongoDB.
9. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Valkey.

## Решение

Выполнен обязательный объем лабораторной:

1. `docker-compose.yml` - PostgreSQL 16, ClickHouse 24.8 и Spark 3.5.1.
2. `sql/postgres/01_create_raw.sql` - создание сырой таблицы `mock_data`.
3. `sql/postgres/02_load_raw.sql` - загрузка всех 10 CSV в PostgreSQL.
4. `sql/postgres/03_create_star_schema.sql` - DDL снежинки/звезды в PostgreSQL.
5. `jobs/etl_to_postgres_star.py` - Spark ETL из `mock_data` в измерения и `fact_sales`.
6. `sql/clickhouse/01_reports_schema.sql` - таблицы отчетов в ClickHouse.
7. `jobs/build_clickhouse_reports.py` - Spark ETL из снежинки PostgreSQL в 6 витрин ClickHouse.

Опциональные Cassandra, Neo4j, MongoDB и Valkey не добавлены.

## Покрытие отчетов ClickHouse

| Требование | Таблица / поле |
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

PostgreSQL для DBeaver:

- host: `localhost`
- port: `5433`
- database: `lab2`
- user: `lab`
- password: `lab`

ClickHouse:

- HTTP port: `8123`
- native port: `9000`
- database: `reports`
- user: `default`
- password: `lab`

## Spark-джобы

Сначала построить снежинку/звезду в PostgreSQL:

```bash
docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/etl_to_postgres_star.py
```

Затем построить 6 отчетов в ClickHouse:

```bash
docker compose exec -T spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /opt/spark/jobs/build_clickhouse_reports.py
```

## Проверка

```bash
docker compose exec -T postgres psql -U lab -d lab2 -c "SELECT COUNT(*) FROM mock_data;"
docker compose exec -T postgres psql -U lab -d lab2 -c "SELECT COUNT(*) FROM fact_sales;"
docker compose exec -T clickhouse clickhouse-client --password lab --query "SELECT count() FROM reports.product_sales_report"
docker compose exec -T clickhouse clickhouse-client --password lab --query "SELECT * FROM reports.time_sales_report ORDER BY year_number, month_number LIMIT 10"
```
