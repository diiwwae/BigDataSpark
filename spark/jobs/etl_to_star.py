from pyspark.sql import functions as F

from common import build_spark, hash_key, read_postgres_table, to_date, to_decimal, to_double, to_int, blank_to_null, write_postgres_table

CUSTOMER_COLUMNS = [
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
]

SELLER_COLUMNS = [
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
]

SUPPLIER_COLUMNS = [
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]

STORE_COLUMNS = [
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
]

PRODUCT_COLUMNS = [
    "product_name",
    "product_category",
    "product_price",
    "product_inventory_quantity",
    "pet_category",
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
]


def build_staging_frame(raw_df):
    cleaned_df = raw_df.select(
        to_int("id").alias("source_row_id"),
        to_int("sale_customer_id").alias("source_customer_id"),
        to_int("sale_seller_id").alias("source_seller_id"),
        to_int("sale_product_id").alias("source_product_id"),
        blank_to_null("customer_first_name").alias("customer_first_name"),
        blank_to_null("customer_last_name").alias("customer_last_name"),
        to_int("customer_age").alias("customer_age"),
        blank_to_null("customer_email").alias("customer_email"),
        blank_to_null("customer_country").alias("customer_country"),
        blank_to_null("customer_postal_code").alias("customer_postal_code"),
        blank_to_null("customer_pet_type").alias("customer_pet_type"),
        blank_to_null("customer_pet_name").alias("customer_pet_name"),
        blank_to_null("customer_pet_breed").alias("customer_pet_breed"),
        blank_to_null("seller_first_name").alias("seller_first_name"),
        blank_to_null("seller_last_name").alias("seller_last_name"),
        blank_to_null("seller_email").alias("seller_email"),
        blank_to_null("seller_country").alias("seller_country"),
        blank_to_null("seller_postal_code").alias("seller_postal_code"),
        blank_to_null("supplier_name").alias("supplier_name"),
        blank_to_null("supplier_contact").alias("supplier_contact"),
        blank_to_null("supplier_email").alias("supplier_email"),
        blank_to_null("supplier_phone").alias("supplier_phone"),
        blank_to_null("supplier_address").alias("supplier_address"),
        blank_to_null("supplier_city").alias("supplier_city"),
        blank_to_null("supplier_country").alias("supplier_country"),
        blank_to_null("store_name").alias("store_name"),
        blank_to_null("store_location").alias("store_location"),
        blank_to_null("store_city").alias("store_city"),
        blank_to_null("store_state").alias("store_state"),
        blank_to_null("store_country").alias("store_country"),
        blank_to_null("store_phone").alias("store_phone"),
        blank_to_null("store_email").alias("store_email"),
        blank_to_null("product_name").alias("product_name"),
        blank_to_null("product_category").alias("product_category"),
        to_decimal("product_price").alias("product_price"),
        to_int("product_quantity").alias("product_inventory_quantity"),
        blank_to_null("pet_category").alias("pet_category"),
        to_decimal("product_weight").alias("product_weight"),
        blank_to_null("product_color").alias("product_color"),
        blank_to_null("product_size").alias("product_size"),
        blank_to_null("product_brand").alias("product_brand"),
        blank_to_null("product_material").alias("product_material"),
        blank_to_null("product_description").alias("product_description"),
        to_double("product_rating").alias("product_rating"),
        to_int("product_reviews").alias("product_reviews"),
        to_date("product_release_date").alias("product_release_date"),
        to_date("product_expiry_date").alias("product_expiry_date"),
        to_date("sale_date").alias("sale_date"),
        to_int("sale_quantity").alias("sale_quantity"),
        to_decimal("sale_total_price").alias("sale_total_price"),
    )

    staged_df = (
        cleaned_df.withColumn("customer_key", hash_key(*CUSTOMER_COLUMNS))
        .withColumn("seller_key", hash_key(*SELLER_COLUMNS))
        .withColumn("supplier_key", hash_key(*SUPPLIER_COLUMNS))
        .withColumn("store_key", hash_key(*STORE_COLUMNS))
        .withColumn("product_key", hash_key(*PRODUCT_COLUMNS, "supplier_key"))
        .withColumn(
            "date_key",
            F.when(F.col("sale_date").isNull(), F.lit(None)).otherwise(
                F.date_format("sale_date", "yyyyMMdd").cast("int")
            ),
        )
        .withColumn(
            "sale_key",
            hash_key(
                "source_row_id",
                "customer_key",
                "seller_key",
                "supplier_key",
                "store_key",
                "product_key",
                "sale_date",
                "sale_quantity",
                "sale_total_price",
            ),
        )
    )

    return staged_df


def main():
    spark = build_spark("bigdataspark-etl-to-star")

    raw_df = read_postgres_table(spark, "raw.mock_data")
    staged_df = build_staging_frame(raw_df).cache()

    dim_customer = (
        staged_df.select(
            "customer_key",
            "source_customer_id",
            F.concat_ws(" ", "customer_first_name", "customer_last_name").alias(
                "customer_full_name"
            ),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        )
        .dropDuplicates(["customer_key"])
    )

    dim_seller = (
        staged_df.select(
            "seller_key",
            "source_seller_id",
            F.concat_ws(" ", "seller_first_name", "seller_last_name").alias(
                "seller_full_name"
            ),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_key"])
    )

    dim_supplier = (
        staged_df.select(
            "supplier_key",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_key"])
    )

    dim_store = (
        staged_df.select(
            "store_key",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_key"])
    )

    dim_product = (
        staged_df.select(
            "product_key",
            "source_product_id",
            "supplier_key",
            "product_name",
            "product_category",
            "product_price",
            "product_inventory_quantity",
            "pet_category",
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
        )
        .dropDuplicates(["product_key"])
    )

    dim_date = (
        staged_df.where(F.col("sale_date").isNotNull())
        .select(
            "date_key",
            F.col("sale_date").alias("full_date"),
            F.dayofmonth("sale_date").alias("day_of_month"),
            F.month("sale_date").alias("month"),
            F.date_format("sale_date", "MMMM").alias("month_name"),
            F.quarter("sale_date").alias("quarter"),
            F.year("sale_date").alias("year"),
        )
        .dropDuplicates(["date_key"])
    )

    fact_sales = (
        staged_df.select(
            "sale_key",
            "source_row_id",
            "source_customer_id",
            "source_seller_id",
            "source_product_id",
            "customer_key",
            "seller_key",
            "store_key",
            "product_key",
            "date_key",
            "sale_quantity",
            "sale_total_price",
            F.round(
                F.when(F.col("sale_quantity") > 0, F.col("sale_total_price") / F.col("sale_quantity")),
                2,
            ).alias("unit_price"),
            F.current_timestamp().alias("loaded_at"),
        )
        .dropDuplicates(["sale_key"])
    )

    write_postgres_table(dim_customer, "dwh.dim_customer")
    write_postgres_table(dim_seller, "dwh.dim_seller")
    write_postgres_table(dim_supplier, "dwh.dim_supplier")
    write_postgres_table(dim_store, "dwh.dim_store")
    write_postgres_table(dim_product, "dwh.dim_product")
    write_postgres_table(dim_date, "dwh.dim_date")
    write_postgres_table(fact_sales, "dwh.fact_sales")

    staged_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
