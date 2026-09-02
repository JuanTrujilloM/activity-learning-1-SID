import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)

SALES_SCHEMA = StructType([
    StructField("card_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", StringType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("card_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("lastname", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("country", StringType(), True),
])

CORRUPT_COL = "_corrupt_record"

TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

DUPLICATE_KEY = ["card_id", "customer_id", "price", "product_id", "timestamp"]

CURATED_COLUMNS = [
    "card_id", "customer_id", "price", "product_id", "sale_ts", "country",
    "source_file", "batch_id", "ingestion_date", "processed_at", "sale_date",
]

def quality_rules(ingestion_date: str):
    rules = [
        (F.col(CORRUPT_COL).isNotNull(), "CORRUPT_RECORD"),
        (F.col("card_id").isNull(), "NULL_CARD_ID"),
        (F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        (F.col("product_id").isNull(), "NULL_PRODUCT_ID"),
        (F.col("price").isNull(), "NULL_PRICE"),
        (F.col("price") <= 0, "NON_POSITIVE_PRICE"),
        (F.col("sale_ts").isNull(), "INVALID_TIMESTAMP"),
        (F.col("sale_ts") > F.lit(ingestion_date).cast("date") + F.expr("INTERVAL 1 DAY"),
         "FUTURE_SALE"),
        (F.col("country").isNull(), "CUSTOMER_NOT_FOUND"),
        (F.col("occurrence") > 1, "DUPLICATE"),
    ]

    return rules


def read_batch(spark: SparkSession, input_path: str) -> DataFrame:
    batch_schema = StructType(SALES_SCHEMA.fields + [StructField(CORRUPT_COL, StringType(), True)])

    batch = (
        spark.read.option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(batch_schema)
        .csv(input_path)
    )

    return batch


def read_customers(spark: SparkSession, customers_path: str) -> DataFrame:
    customers = (
        spark.read.option("header", "true")
        .schema(CUSTOMERS_SCHEMA)
        .csv(customers_path)
        .select("customer_id", "country")
        .where(F.col("customer_id").isNotNull())
    )

    return customers

def validate_batch(batch: DataFrame, customers: DataFrame, batch_id: str, ingestion_date: str) -> DataFrame:
    with_metadata = (
        batch
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("sale_ts", F.to_timestamp("timestamp", TS_FORMAT))
    )

    with_customer = with_metadata.join(customers, on="customer_id", how="left")

    arrival_order = Window.partitionBy(*DUPLICATE_KEY).orderBy(F.monotonically_increasing_id())
    with_occurrence = with_customer.withColumn("occurrence", F.row_number().over(arrival_order))

    reason_columns = []
    for condition, reason in quality_rules(ingestion_date):
        reason_columns.append(F.when(condition, F.lit(reason)))

    validated = with_occurrence.withColumn(
        "reject_reasons", F.array_compact(F.array(*reason_columns))
    )

    return validated


def to_curated(accepted: DataFrame) -> DataFrame:
    curated = (
        accepted
        .withColumn("country", F.upper(F.trim(F.regexp_replace("country", "_", " "))))
        .withColumn("sale_date", F.to_date("sale_ts"))
        .select(*CURATED_COLUMNS)
    )
    
    return curated


def to_quarantine(rejected: DataFrame) -> DataFrame:
    quarantined = rejected.select(
        "card_id", "customer_id", "price", "product_id",
        F.col("timestamp").alias("sale_ts_raw"),
        F.col(CORRUPT_COL).alias("raw_record"),
        "reject_reasons", "source_file", "batch_id", "ingestion_date", "processed_at",
    )

    return quarantined

def write_curated(spark: SparkSession, df_new: DataFrame, path: str, partition_col, batch_id: str) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try:
        existing = spark.read.parquet(path)
    except Exception:
        existing = None

    out = df_new
    if existing is not None:
        survivors = existing.where(F.col("batch_id") != batch_id)

        if partition_col:
            partition_rows = df_new.select(partition_col).distinct().collect()

            touched = []
            for row in partition_rows:
                touched.append(row[0])

            survivors = survivors.where(F.col(partition_col).isin(touched))

        survivors = survivors.localCheckpoint(eager=True)
        out = df_new.unionByName(survivors)

    writer = out.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)


def write_quarantine(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").partitionBy("batch_id").parquet(path)

def build_metrics(validated: DataFrame, batch_id: str, ingestion_date: str, started: float) -> dict:
    is_rejected = F.size("reject_reasons") > 0

    totals = validated.select(
        F.count("*").alias("input"),
        F.count_if(~is_rejected).alias("accepted"),
        F.count_if(is_rejected).alias("rejected"),
    ).first()

    reason_rows = (
        validated.select(F.explode("reject_reasons").alias("reason"))
        .groupBy("reason")
        .agg(F.count("*").alias("n"))
        .orderBy(F.desc("n"))
        .collect()
    )

    by_reason = {}
    for row in reason_rows:
        by_reason[row["reason"]] = row["n"]

    metrics = {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_seconds": round(datetime.now(timezone.utc).timestamp() - started, 1),
        "input_records": totals["input"],
        "accepted_records": totals["accepted"],
        "rejected_records": totals["rejected"],
        "duplicate_records": by_reason.get("DUPLICATE", 0),
        "rejections_by_reason": by_reason,
        "reconciled": totals["input"] == totals["accepted"] + totals["rejected"],
    }
    
    return metrics


def write_metrics(spark: SparkSession, metrics: dict, metrics_path: str) -> None:
    output_path = metrics_path.rstrip("/") + f"/batch_id={metrics['batch_id']}"
    (
        spark.createDataFrame([[json.dumps(metrics)]], "value string")
        .coalesce(1)
        .write.mode("overwrite")
        .text(output_path)
    )

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Valida y transforma un lote de ventas.")
    p.add_argument("--input-path", required=True, help="Lote en la zona de datos originales")
    p.add_argument("--customers-path", required=True, help="Dimension de clientes")
    p.add_argument("--batch-id", required=True, help="Identificador unico del lote")
    p.add_argument("--ingestion-date", required=True, help="Fecha de ingestion (YYYY-MM-DD)")
    p.add_argument("--curated-output-path", required=True)
    p.add_argument("--quarantine-output-path", required=True)
    p.add_argument("--metrics-output-path", required=True)
    p.add_argument(
        "--partition-by",
        default="sale_date",
        choices=["sale_date", "ingestion_date", "none"],
        help="Particionamiento de la zona curada (none = sin particionar)",
    )
    args = p.parse_args(argv)
    started = datetime.now(timezone.utc).timestamp()

    spark = SparkSession.builder.appName(f"validate_sales_batch::{args.batch_id}").getOrCreate()

    try:
        batch = read_batch(spark, args.input_path)
        customers = read_customers(spark, args.customers_path)
        validated = validate_batch(batch, customers, args.batch_id, args.ingestion_date).cache()

        metrics = build_metrics(validated, args.batch_id, args.ingestion_date, started)
        if metrics["input_records"] == 0:
            print(f"ERROR: el lote {args.batch_id} no contiene registros.", file=sys.stderr)
            return 1
        if not metrics["reconciled"]:
            print(f"ERROR: descuadre de reconciliacion en {args.batch_id}.", file=sys.stderr)
            return 1

        accepted = validated.where(F.size("reject_reasons") == 0)
        rejected = validated.where(F.size("reject_reasons") > 0)

        curated = to_curated(accepted)
        quarantined = to_quarantine(rejected)

        if args.partition_by == "none":
            partitioned = None
        else:
            partitioned = args.partition_by

        write_curated(spark, curated, args.curated_output_path, partitioned, args.batch_id)
        write_quarantine(quarantined, args.quarantine_output_path)
        write_metrics(spark, metrics, args.metrics_output_path)

        print("\n METRICAS DEL LOTE")
        print(json.dumps(metrics, indent=2, ensure_ascii=False))
        print(
            f"RECONCILIACION: {metrics['input_records']} entrada = "
            f"{metrics['accepted_records']} aceptados + {metrics['rejected_records']} rechazados"
            f"  (de los cuales {metrics['duplicate_records']} son duplicados)\n"
        )
        return 0

    except Exception as exc:
        print(f"ERROR procesando el lote {args.batch_id}: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
