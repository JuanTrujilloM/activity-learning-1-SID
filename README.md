# activity-learning-1-SID

Jobs de PySpark del data lake de ventas. Corren igual en local y en Amazon EMR: no
fijan `master` y todas las rutas pasan por Spark, que resuelve igual una ruta local
que una `s3://`.

## `scripts/validate_sales_batch.py`

Procesa **un** lote de ventas: lo lee de la zona de datos originales, aplica las
reglas de calidad, separa los registros aceptados de los rechazados y escribe tres
salidas — la zona curada en Parquet, la cuarentena particionada por lote y las
métricas del lote en JSON.

Reglas de calidad: identificadores nulos, precios nulos o no positivos, fechas
inválidas o futuras, clientes inexistentes en la dimensión y duplicados exactos. Un
registro conserva todas sus razones de rechazo.

Devuelve `0` si el lote se procesó y reconcilió; `1` si está vacío, si el encabezado
no cumple el contrato de esquema o si la reconciliación no cuadra.

```bash
spark-submit --master yarn --deploy-mode cluster validate_sales_batch.py \
  --input-path s3://BUCKET/raw/sales/sales_batch_2026-08-24.csv \
  --customers-path s3://BUCKET/raw/customers/ \
  --batch-id sales_2026-08-24 --ingestion-date 2026-08-24 \
  --curated-output-path s3://BUCKET/curated/sales_parquet_partitioned \
  --quarantine-output-path s3://BUCKET/quarantine/sales \
  --metrics-output-path s3://BUCKET/results/metrics/sales
```

| Parámetro | Descripción |
|---|---|
| `--input-path` | El archivo del lote, no el directorio |
| `--customers-path` | Dimensión de clientes: integridad referencial y país |
| `--batch-id` | Identificador del lote, queda en cada registro |
| `--ingestion-date` | Fecha de ingestión, distinta de la del evento |
| `--partition-by` | `sale_date` (defecto), `ingestion_date` o `none` |

## `scripts/build_web_product.py`

Genera el producto analítico que consume el sitio web: lee la zona curada, agrega
ventas, ingresos y ticket promedio por país, y escribe un JSON único en
`products/web/sales_by_country.json`.

No recibe parámetros — las rutas son constantes al inicio del archivo, porque un
producto de datos tiene una ruta fija por definición. Escribe con el sistema de
archivos de Hadoop en lugar de `DataFrame.write.json()`, que produciría un directorio
con archivos `part-<uuid>.json`: el navegador necesita una URL estable.

```bash
spark-submit --master yarn --deploy-mode cluster build_web_product.py
```
