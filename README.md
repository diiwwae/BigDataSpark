# BigDataSpark

ETL-пайплайн на Apache Spark для лабораторной работы №2:

- исходные CSV автоматически загружаются в PostgreSQL в таблицу `raw.mock_data`
- Spark-джоба строит модель данных звезда в PostgreSQL
- вторая Spark-джоба строит 6 витрин и сохраняет их в ClickHouse

Реализовано: `PostgreSQL + Spark + ClickHouse`.

## Структура проекта

```text
BigDataSpark/
├── clickhouse/init/
│   └── 01_create_database.sql
├── docker-compose.yml
├── postgres/init/
│   ├── 01_create_schemas.sql
│   └── 02_load_mock_data.sh
├── scripts/
│   ├── run_all.sh
│   ├── run_clickhouse.sh
│   └── run_star.sh
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       ├── common.py
│       ├── etl_to_clickhouse.py
│       └── etl_to_star.py
├── report.md
└── исходные данные/*.csv
```

## Архитектура

### 1. Слой raw в PostgreSQL

При старте контейнера PostgreSQL:

- создаются схемы `raw` и `dwh`
- создаётся таблица `raw.mock_data`
- все 10 CSV-файлов загружаются в `raw.mock_data`

Ожидаемый объём: `10000` строк.

### 2. Звезда в PostgreSQL

Spark-джоба `spark/jobs/etl_to_star.py` создаёт таблицы:

- `dwh.dim_customer`
- `dwh.dim_seller`
- `dwh.dim_supplier`
- `dwh.dim_store`
- `dwh.dim_product`
- `dwh.dim_date`
- `dwh.fact_sales`

### 3. Витрины в ClickHouse

При старте контейнера ClickHouse:

- выполняются init-скрипты из `clickhouse/init`
- создаётся база данных `analytics`

Spark-джоба `spark/jobs/etl_to_clickhouse.py` создаёт 6 таблиц:

- `report_sales_products`
- `report_sales_customers`
- `report_sales_time`
- `report_sales_stores`
- `report_sales_suppliers`
- `report_product_quality`

## Какие витрины реализованы

### `report_sales_products`

Показывает:

- выручку по продуктам
- количество продаж
- средний рейтинг
- количество отзывов
- выручку по категории
- ранг продукта и признак Top-10

### `report_sales_customers`

Показывает:

- общую сумму покупок клиента
- количество заказов
- средний чек
- распределение клиентов по странам
- ранг клиента и признак Top-10

### `report_sales_time`

Показывает:

- месячные и годовые тренды
- выручку по периодам
- средний размер заказа
- сравнение с предыдущим периодом
- рост выручки в процентах

### `report_sales_stores`

Показывает:

- выручку магазина
- средний чек
- распределение продаж по городам и странам
- ранг магазина и признак Top-5

### `report_sales_suppliers`

Показывает:

- выручку поставщика
- среднюю цену товаров поставщика
- распределение по странам поставщиков
- ранг поставщика и признак Top-5

### `report_product_quality`

Показывает:

- продукты с самым высоким и низким рейтингом
- количество отзывов
- объём продаж
- глобальную корреляцию между рейтингом и объёмом продаж

## Запуск

### 1. Поднять окружение

Из корня `BigDataSpark`:

```bash
docker compose up -d --build
```

Проверить, что контейнеры поднялись:

```bash
docker compose ps
```

### 2. Построить звезду в PostgreSQL

```bash
./scripts/run_star.sh
```

### 3. Построить витрины в ClickHouse

```bash
./scripts/run_clickhouse.sh
```

### 4. Полный запуск

```bash
./scripts/run_all.sh
```

## Проверка результата

### PostgreSQL

Проверить количество строк в raw:

```bash
docker compose exec postgres psql -U postgres -d pet_shop -c "SELECT COUNT(*) FROM raw.mock_data;"
```

Проверить таблицы звезды:

```bash
docker compose exec postgres psql -U postgres -d pet_shop -c "\dt dwh.*"
```

### ClickHouse

Проверить список витрин:

```bash
docker compose exec clickhouse clickhouse-client --database analytics --query "SHOW TABLES"
```

Пример запроса:

```bash
docker compose exec clickhouse clickhouse-client --query "
SELECT product_name, product_brand, total_revenue, sales_rank
FROM analytics.report_sales_products
ORDER BY sales_rank
LIMIT 10
"
```
