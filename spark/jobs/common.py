import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types as T

DATE_FORMAT = "M/d/yyyy"
JDBC_CLASSPATH = (
    "/opt/spark/jars/postgresql-42.7.4.jar:"
    "/opt/spark/jars/clickhouse-jdbc-0.6.3.jar"
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(env("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.session.timeZone", env("SPARK_TIMEZONE", "UTC"))
        .config(
            "spark.driver.extraClassPath",
            env("SPARK_JDBC_CLASSPATH", JDBC_CLASSPATH),
        )
        .config(
            "spark.executor.extraClassPath",
            env("SPARK_JDBC_CLASSPATH", JDBC_CLASSPATH),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "WARN"))
    return spark


def postgres_properties() -> dict[str, str]:
    return {
        "user": env("POSTGRES_USER", "postgres"),
        "password": env("POSTGRES_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }


def clickhouse_properties() -> dict[str, str]:
    return {
        "user": env("CLICKHOUSE_USER", "default"),
        "password": env("CLICKHOUSE_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }


def read_postgres_table(spark: SparkSession, table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url", env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/pet_shop"))
        .option("dbtable", table_name)
        .options(**postgres_properties())
        .load()
    )


def write_postgres_table(df, table_name: str, mode: str = "overwrite") -> None:
    (
        df.repartition(int(env("JDBC_WRITE_PARTITIONS", "1")))
        .write.format("jdbc")
        .mode(mode)
        .option("url", env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/pet_shop"))
        .option("dbtable", table_name)
        .options(**postgres_properties())
        .save()
    )


def write_clickhouse_table(
    df,
    table_name: str,
    create_table_options: str,
    mode: str = "overwrite",
) -> None:
    prepared_df = prepare_for_clickhouse(df)
    writer = (
        prepared_df.repartition(int(env("JDBC_WRITE_PARTITIONS", "1")))
        .write.format("jdbc")
        .mode(mode)
        .option(
            "url",
            env("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/analytics"),
        )
        .option("dbtable", table_name)
        .option("createTableOptions", create_table_options)
        .options(**clickhouse_properties())
    )
    writer.save()


def prepare_for_clickhouse(df):
    numeric_types = (
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )

    prepared_columns = []
    for field in df.schema.fields:
        column = F.col(field.name)
        data_type = field.dataType

        if isinstance(data_type, T.StringType):
            prepared_columns.append(F.coalesce(column, F.lit("")).alias(field.name))
        elif isinstance(data_type, numeric_types):
            prepared_columns.append(
                F.coalesce(column, F.lit(0).cast(data_type)).alias(field.name)
            )
        elif isinstance(data_type, T.BooleanType):
            prepared_columns.append(F.coalesce(column, F.lit(False)).alias(field.name))
        else:
            prepared_columns.append(column.alias(field.name))

    return df.select(*prepared_columns)


def _ensure_column(value):
    return F.col(value) if isinstance(value, str) else value


def blank_to_null(value):
    column = _ensure_column(value).cast("string")
    return F.when(F.trim(column) == "", F.lit(None)).otherwise(F.trim(column))


def to_int(column_name: str):
    return blank_to_null(column_name).cast("int")


def to_decimal(column_name: str, precision: int = 18, scale: int = 2):
    return blank_to_null(column_name).cast(f"decimal({precision},{scale})")


def to_double(column_name: str):
    return blank_to_null(column_name).cast("double")


def to_date(column_name: str, fmt: str = DATE_FORMAT):
    return F.to_date(blank_to_null(column_name), fmt)


def hash_key(*columns):
    prepared_columns = [
        F.coalesce(blank_to_null(column).cast("string"), F.lit("")) for column in columns
    ]
    return F.sha2(F.concat_ws("||", *prepared_columns), 256)
