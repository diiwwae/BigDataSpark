# Отчёт по лабораторной работе

## Цель

Построить ETL-пайплайн на Apache Spark:

1. загрузить сырые данные из CSV в PostgreSQL
2. преобразовать их в модель данных звезда в PostgreSQL
3. построить аналитические витрины и сохранить их в ClickHouse

## Источник данных

Источник: 10 CSV-файлов `MOCK_DATA*.csv`, по 1000 строк в каждом.

Всего: `10000` записей.

## Особенность исходных данных

Поля:

- `sale_customer_id`
- `sale_seller_id`
- `sale_product_id`

повторяются между файлами и не могут использоваться как глобальные бизнес-ключи. Поэтому в модели звезда используются surrogate key, рассчитанные как SHA-256-хеш атрибутов соответствующей сущности.

## Реализованная модель звезда

### Измерения

- `dim_customer`
- `dim_seller`
- `dim_supplier`
- `dim_store`
- `dim_product`
- `dim_date`

### Факт

- `fact_sales`

Факт содержит:

- ссылки на измерения
- количество проданных единиц
- итоговую сумму продажи
- расчётную цену за единицу
- исходные идентификаторы для трассировки

## Реализованные витрины в ClickHouse

База данных ClickHouse: `analytics`.

### `report_sales_products`

Гранулярность: продуктовая группа `product_name + product_brand + product_category`.

Метрики:

- `total_revenue`
- `total_sales_quantity`
- `order_count`
- `avg_rating`
- `total_reviews`
- `category_revenue`
- `sales_rank`

### `report_sales_customers`

Гранулярность: клиент.

Метрики:

- `total_spent`
- `order_count`
- `avg_check`
- `total_items`
- `country_customer_count`
- `customer_rank`

### `report_sales_time`

Гранулярность: месяц.

Метрики:

- `total_revenue`
- `year_total_revenue`
- `order_count`
- `avg_order_size`
- `total_items`
- `previous_period_revenue`
- `revenue_diff_prev_period`
- `revenue_growth_pct`

### `report_sales_stores`

Гранулярность: магазин.

Метрики:

- `total_revenue`
- `order_count`
- `avg_check`
- `total_items`
- `city_revenue`
- `country_revenue`
- `store_rank`

### `report_sales_suppliers`

Гранулярность: поставщик.

Метрики:

- `total_revenue`
- `avg_product_price`
- `order_count`
- `total_items`
- `country_revenue`
- `country_supplier_count`
- `supplier_rank`

### `report_product_quality`

Гранулярность: продуктовая группа `product_name + product_brand + product_category`.

Метрики:

- `avg_rating`
- `total_reviews`
- `total_sales_quantity`
- `total_revenue`
- `sales_rating_correlation`
- `highest_rating_rank`
- `lowest_rating_rank`
- `reviews_rank`

## Проверочные запросы

### PostgreSQL

```sql
SELECT COUNT(*) FROM raw.mock_data;
SELECT COUNT(*) FROM dwh.fact_sales;
SELECT COUNT(*) FROM dwh.dim_product;
```

### ClickHouse

```sql
SELECT * FROM report_sales_products ORDER BY sales_rank LIMIT 10;
SELECT * FROM report_sales_time ORDER BY year, month;
SELECT * FROM report_product_quality WHERE is_highest_rated = 1 OR is_lowest_rated = 1;
```

## Итог

Реализовано:

- PostgreSQL
- Apache Spark
- ClickHouse
- 6 аналитических витрин
