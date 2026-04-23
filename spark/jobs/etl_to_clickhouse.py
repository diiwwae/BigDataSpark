from pyspark.sql import Window
from pyspark.sql import functions as F

from common import build_spark, hash_key, read_postgres_table, write_clickhouse_table

PRODUCT_GROUP_COLUMNS = ["product_name", "product_brand", "product_category"]


def build_sales_dataset(spark):
    fact_sales = read_postgres_table(spark, "dwh.fact_sales").alias("f")
    dim_customer = read_postgres_table(spark, "dwh.dim_customer").alias("c")
    dim_product = read_postgres_table(spark, "dwh.dim_product").alias("p")
    dim_supplier = read_postgres_table(spark, "dwh.dim_supplier").alias("sup")
    dim_store = read_postgres_table(spark, "dwh.dim_store").alias("st")
    dim_date = read_postgres_table(spark, "dwh.dim_date").alias("d")

    return (
        fact_sales.join(dim_customer, "customer_key", "left")
        .join(dim_product, "product_key", "left")
        .join(dim_supplier, F.col("p.supplier_key") == F.col("sup.supplier_key"), "left")
        .join(dim_store, "store_key", "left")
        .join(dim_date, "date_key", "left")
        .select(
            F.col("f.sale_key").alias("sale_key"),
            F.col("f.sale_quantity").cast("long").alias("sale_quantity"),
            F.col("f.sale_total_price").cast("double").alias("sale_total_price"),
            F.col("f.unit_price").cast("double").alias("unit_price"),
            F.col("c.customer_full_name").alias("customer_full_name"),
            F.col("c.customer_email").alias("customer_email"),
            F.col("c.customer_country").alias("customer_country"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.product_brand").alias("product_brand"),
            F.col("p.product_category").alias("product_category"),
            F.col("p.product_price").cast("double").alias("product_price"),
            F.col("p.product_rating").cast("double").alias("product_rating"),
            F.col("p.product_reviews").cast("long").alias("product_reviews"),
            F.col("sup.supplier_name").alias("supplier_name"),
            F.col("sup.supplier_country").alias("supplier_country"),
            F.col("st.store_name").alias("store_name"),
            F.col("st.store_location").alias("store_location"),
            F.col("st.store_city").alias("store_city"),
            F.col("st.store_state").alias("store_state"),
            F.col("st.store_country").alias("store_country"),
            F.col("d.full_date").alias("full_date"),
            F.col("d.month").alias("month"),
            F.col("d.month_name").alias("month_name"),
            F.col("d.year").alias("year"),
        )
    )


def build_product_sales_mart(base_df):
    sales_rank_window = Window.orderBy(
        F.desc("total_sales_quantity"), F.desc("total_revenue")
    )

    return (
        base_df.groupBy(*PRODUCT_GROUP_COLUMNS)
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.sum("sale_quantity").alias("total_sales_quantity"),
            F.countDistinct("sale_key").alias("order_count"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("product_reviews").alias("total_reviews"),
        )
        .withColumn("product_group_key", hash_key(*PRODUCT_GROUP_COLUMNS))
        .withColumn(
            "category_revenue",
            F.round(F.sum("total_revenue").over(Window.partitionBy("product_category")), 2),
        )
        .withColumn("sales_rank", F.dense_rank().over(sales_rank_window))
        .withColumn("is_top_10_product", F.col("sales_rank") <= F.lit(10))
        .select(
            "product_group_key",
            "product_name",
            "product_brand",
            "product_category",
            "total_revenue",
            "total_sales_quantity",
            "order_count",
            "avg_rating",
            "total_reviews",
            "category_revenue",
            "sales_rank",
            "is_top_10_product",
        )
    )


def build_customer_sales_mart(base_df):
    rank_window = Window.orderBy(F.desc("total_spent"), F.desc("order_count"))

    return (
        base_df.groupBy("customer_email", "customer_full_name", "customer_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.countDistinct("sale_key").alias("order_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
            F.sum("sale_quantity").alias("total_items"),
        )
        .withColumn("customer_key", hash_key("customer_email"))
        .withColumn(
            "country_customer_count",
            F.count("customer_email").over(Window.partitionBy("customer_country")),
        )
        .withColumn("customer_rank", F.dense_rank().over(rank_window))
        .withColumn("is_top_10_customer", F.col("customer_rank") <= F.lit(10))
        .select(
            "customer_key",
            "customer_full_name",
            "customer_email",
            "customer_country",
            "total_spent",
            "order_count",
            "avg_check",
            "total_items",
            "country_customer_count",
            "customer_rank",
            "is_top_10_customer",
        )
    )


def build_time_sales_mart(base_df):
    period_window = Window.orderBy("year", "month")

    return (
        base_df.groupBy("year", "month", "month_name")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_key").alias("order_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
            F.sum("sale_quantity").alias("total_items"),
        )
        .withColumn("year_month", F.format_string("%04d-%02d", F.col("year"), F.col("month")))
        .withColumn(
            "year_total_revenue",
            F.round(F.sum("total_revenue").over(Window.partitionBy("year")), 2),
        )
        .withColumn(
            "previous_period_revenue",
            F.coalesce(F.lag("total_revenue").over(period_window), F.lit(0.0)),
        )
        .withColumn(
            "revenue_diff_prev_period",
            F.round(F.col("total_revenue") - F.col("previous_period_revenue"), 2),
        )
        .withColumn(
            "revenue_growth_pct",
            F.round(
                F.when(
                    F.col("previous_period_revenue") == 0,
                    F.lit(0.0),
                ).otherwise(
                    (
                        (F.col("total_revenue") - F.col("previous_period_revenue"))
                        / F.col("previous_period_revenue")
                    )
                    * 100
                ),
                2,
            ),
        )
        .select(
            "year",
            "month",
            "month_name",
            "year_month",
            "total_revenue",
            "year_total_revenue",
            "order_count",
            "avg_order_size",
            "total_items",
            "previous_period_revenue",
            "revenue_diff_prev_period",
            "revenue_growth_pct",
        )
    )


def build_store_sales_mart(base_df):
    rank_window = Window.orderBy(F.desc("total_revenue"), F.desc("order_count"))

    return (
        base_df.groupBy(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_key").alias("order_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
            F.sum("sale_quantity").alias("total_items"),
        )
        .withColumn(
            "store_key",
            hash_key(
                "store_name",
                "store_location",
                "store_city",
                "store_state",
                "store_country",
            ),
        )
        .withColumn(
            "city_revenue",
            F.round(
                F.sum("total_revenue").over(Window.partitionBy("store_city", "store_country")),
                2,
            ),
        )
        .withColumn(
            "country_revenue",
            F.round(F.sum("total_revenue").over(Window.partitionBy("store_country")), 2),
        )
        .withColumn("store_rank", F.dense_rank().over(rank_window))
        .withColumn("is_top_5_store", F.col("store_rank") <= F.lit(5))
        .select(
            "store_key",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "total_revenue",
            "order_count",
            "avg_check",
            "total_items",
            "city_revenue",
            "country_revenue",
            "store_rank",
            "is_top_5_store",
        )
    )


def build_supplier_sales_mart(base_df):
    rank_window = Window.orderBy(F.desc("total_revenue"), F.desc("order_count"))

    return (
        base_df.groupBy("supplier_name", "supplier_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.countDistinct("sale_key").alias("order_count"),
            F.sum("sale_quantity").alias("total_items"),
        )
        .withColumn("supplier_key", hash_key("supplier_name", "supplier_country"))
        .withColumn(
            "country_revenue",
            F.round(F.sum("total_revenue").over(Window.partitionBy("supplier_country")), 2),
        )
        .withColumn(
            "country_supplier_count",
            F.count("supplier_name").over(Window.partitionBy("supplier_country")),
        )
        .withColumn("supplier_rank", F.dense_rank().over(rank_window))
        .withColumn("is_top_5_supplier", F.col("supplier_rank") <= F.lit(5))
        .select(
            "supplier_key",
            "supplier_name",
            "supplier_country",
            "total_revenue",
            "avg_product_price",
            "order_count",
            "total_items",
            "country_revenue",
            "country_supplier_count",
            "supplier_rank",
            "is_top_5_supplier",
        )
    )


def build_product_quality_mart(base_df):
    highest_rank_window = Window.orderBy(F.desc("avg_rating"), F.desc("total_reviews"))
    lowest_rank_window = Window.orderBy(F.asc("avg_rating"), F.desc("total_reviews"))
    reviews_rank_window = Window.orderBy(F.desc("total_reviews"))

    mart_df = (
        base_df.groupBy(*PRODUCT_GROUP_COLUMNS)
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.sum("product_reviews").alias("total_reviews"),
            F.sum("sale_quantity").alias("total_sales_quantity"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn("product_group_key", hash_key(*PRODUCT_GROUP_COLUMNS))
    )

    correlation_df = mart_df.agg(
        F.round(F.corr("avg_rating", "total_sales_quantity"), 4).alias(
            "sales_rating_correlation"
        )
    )

    return (
        mart_df.crossJoin(correlation_df)
        .withColumn("highest_rating_rank", F.dense_rank().over(highest_rank_window))
        .withColumn("lowest_rating_rank", F.dense_rank().over(lowest_rank_window))
        .withColumn("reviews_rank", F.dense_rank().over(reviews_rank_window))
        .withColumn("is_highest_rated", F.col("highest_rating_rank") == F.lit(1))
        .withColumn("is_lowest_rated", F.col("lowest_rating_rank") == F.lit(1))
        .withColumn("has_most_reviews", F.col("reviews_rank") == F.lit(1))
        .select(
            "product_group_key",
            "product_name",
            "product_brand",
            "product_category",
            "avg_rating",
            "total_reviews",
            "total_sales_quantity",
            "total_revenue",
            "sales_rating_correlation",
            "highest_rating_rank",
            "lowest_rating_rank",
            "reviews_rank",
            "is_highest_rated",
            "is_lowest_rated",
            "has_most_reviews",
        )
    )


def main():
    spark = build_spark("bigdataspark-etl-to-clickhouse")

    base_df = build_sales_dataset(spark).cache()

    write_clickhouse_table(
        build_product_sales_mart(base_df),
        "report_sales_products",
        "ENGINE = MergeTree() ORDER BY (product_group_key)",
    )
    write_clickhouse_table(
        build_customer_sales_mart(base_df),
        "report_sales_customers",
        "ENGINE = MergeTree() ORDER BY (customer_key)",
    )
    write_clickhouse_table(
        build_time_sales_mart(base_df),
        "report_sales_time",
        "ENGINE = MergeTree() ORDER BY (year, month)",
    )
    write_clickhouse_table(
        build_store_sales_mart(base_df),
        "report_sales_stores",
        "ENGINE = MergeTree() ORDER BY (store_key)",
    )
    write_clickhouse_table(
        build_supplier_sales_mart(base_df),
        "report_sales_suppliers",
        "ENGINE = MergeTree() ORDER BY (supplier_key)",
    )
    write_clickhouse_table(
        build_product_quality_mart(base_df),
        "report_product_quality",
        "ENGINE = MergeTree() ORDER BY (product_group_key)",
    )

    base_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
