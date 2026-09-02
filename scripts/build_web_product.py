import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "s3://activitylearning1-383161404298"

CURATED_PATH = f"{BUCKET}/curated/sales_parquet_partitioned"
OUTPUT_PATH = f"{BUCKET}/products/web/sales_by_country.json"

spark = SparkSession.builder.appName("build_web_product").getOrCreate()

curated = spark.read.parquet(CURATED_PATH)

cobertura = curated.select(
    F.min("sale_date").alias("desde"),
    F.max("sale_date").alias("hasta"),
    F.count("*").alias("ventas_totales"),
).first()

rows = (
    curated
    .groupBy("country")
    .agg(
        F.count("*").alias("ventas"),
        F.round(F.sum("price"), 2).alias("ingresos"),
        F.round(F.avg("price"), 2).alias("ticket_promedio"),
    )
    .orderBy(F.desc("ingresos"))
    .collect()
)

datos = []
for row in rows:
    datos.append({
        "country": row["country"],
        "ventas": row["ventas"],
        "ingresos": row["ingresos"],
        "ticket_promedio": row["ticket_promedio"],
    })

product = {
    "generado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "cobertura": {
        "desde": cobertura["desde"].isoformat(),
        "hasta": cobertura["hasta"].isoformat(),
    },
    "ventas_totales": cobertura["ventas_totales"],
    "datos": datos,
}

ruta = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH)
fs = ruta.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())

salida = fs.create(ruta, True)
salida.write(bytearray(json.dumps(product, ensure_ascii=False, indent=2), "utf-8"))
salida.close()

print(json.dumps(product, indent=2, ensure_ascii=False))
print(f"\nEscrito en {OUTPUT_PATH}\n")

spark.stop()
