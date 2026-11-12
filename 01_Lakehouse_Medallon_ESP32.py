# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto Final – Big Data en la Nube (Universidad Inpahu)
# MAGIC ## Data Lakehouse Medallón (Bronze → Silver → Gold) con ingesta IoT desde ESP32
# MAGIC
# MAGIC **Arquitectura**
# MAGIC
# MAGIC | Origen | Capa | Tabla |
# MAGIC |---|---|---|
# MAGIC | Spark (200.000 eventos sintéticos) + ESP32 #1 (eventos en vivo por API REST) | **Bronze** | `bronze.eventos_raw` |
# MAGIC | Limpieza, deduplicación, ventanas, broadcast join | **Silver** | `silver.silver_events` |
# MAGIC | Agregaciones con Liquid Clustering | **Gold** | `gold.gold_sales_summary`, `gold.gold_user_metrics` |
# MAGIC | ESP32 #2 consulta Gold y sirve una página web con gráficas | **Consumo** | Dashboard físico + Dashboard Databricks SQL |
# MAGIC
# MAGIC **Orden de ejecución:** celdas 0 → 6c antes de encender los ESP32. Luego, con el ESP32 #1 enviando datos, ejecutar 9 → 12.

# COMMAND ----------

# Celda 00 (Python) - Configuración: catálogo y esquemas de la arquitectura Medallón
CATALOG = "proyecto_nota3"

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    # En algunas cuentas Free Edition no se permite crear catálogos: se usa el catálogo por defecto
    print(f"No se pudo crear el catálogo '{CATALOG}': {str(e)[:150]}")
    CATALOG = "workspace"

spark.sql(f"USE CATALOG {CATALOG}")
for esquema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{esquema}")

BRONZE = f"{CATALOG}.bronze.eventos_raw"
SILVER = f"{CATALOG}.silver.silver_events"
print(f"Catálogo activo: {CATALOG}  |  Esquemas: bronze, silver, gold")
print(">> Anota este nombre de catálogo: va en la configuración de los dos ESP32.")

# COMMAND ----------

# Celda 0 (Python) - Generación del Dataset Sintético e Ingesta en BRONZE
from pyspark.sql.functions import expr, col, rand, concat, lit, lpad, round, when, lower, current_timestamp

# 1. Generación de 200,000 eventos sintéticos (transformaciones: aún no se ejecuta nada)
df_gen = spark.range(1, 200001) \
    .withColumn('event_id', concat(lit('EVT-'), lpad(col('id').cast('string'), 7, '0'))) \
    .withColumn('user_id', concat(lit('USR-'), lpad(expr("CAST(rand() * 5000 + 1 AS INT)").cast('string'), 5, '0'))) \
    .withColumn('event_type', expr("element_at(array('purchase', 'click', 'view', 'cart', 'refund'), CAST(rand() * 5 + 1 AS INT))")) \
    .withColumn('category', expr("element_at(array('ELECTRONICA', 'HOGAR', 'ROPA', 'DEPORTES', 'JUGUETERIA'), CAST(rand() * 5 + 1 AS INT))")) \
    .withColumn('amount', round(rand() * 450 + 10, 2)) \
    .withColumn('quantity', expr("CAST(rand() * 5 + 1 AS INT)")) \
    .withColumn('region', expr("element_at(array('NORTE', 'SUR', 'ESTE', 'OESTE', 'CENTRO'), CAST(rand() * 5 + 1 AS INT))")) \
    .withColumn('event_timestamp', expr("timestampadd(SECOND, CAST(rand() * 86400 AS INT), to_timestamp(date_add('2026-01-01', CAST(rand() * 280 AS INT))))")) \
    .drop('id')

# 2. Simular "datos sucios" típicos de una capa Bronze (para que Silver tenga algo que limpiar)
df_sucio = df_gen \
    .withColumn('category', when(rand() < 0.02, lower(concat(lit('  '), col('category'), lit(' ')))).otherwise(col('category'))) \
    .withColumn('amount', when(rand() < 0.005, -col('amount')).otherwise(col('amount')))

# ~1% de duplicados (reenvíos del sistema origen)
df_bronze = df_sucio.unionByName(df_sucio.sample(fraction=0.01, seed=42)) \
    .withColumn('source', lit('SPARK_SINTETICO')) \
    .withColumn('ingestion_time', current_timestamp())

# 3. ACCIÓN: persistir en Delta Lake (capa Bronze)
df_bronze.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(BRONZE)

print(f"Bronze cargado: {spark.table(BRONZE).count():,} filas (200.000 eventos + duplicados simulados) en {BRONZE}")
display(spark.table(BRONZE).limit(20))

# COMMAND ----------

# Celda 0b (Python) - Schema Enforcement vs Schema Evolution (prepara Bronze para recibir datos del ESP32)
# El ESP32 envía un campo nuevo: device_id. Delta Lake, por defecto, RECHAZA columnas que no existen.
from pyspark.sql.functions import current_timestamp, lit

df_prueba = spark.table(BRONZE).limit(3) \
    .withColumn('event_id', concat(lit('TEST-'), col('event_id'))) \
    .withColumn('event_type', lit('view')) \
    .withColumn('source', lit('PRUEBA_SCHEMA')) \
    .withColumn('device_id', lit('ESP32-TEST'))

# a) Schema Enforcement (comportamiento por defecto): debe fallar
try:
    df_prueba.write.format('delta').mode('append').saveAsTable(BRONZE)
    print("Inesperado: la escritura pasó sin mergeSchema")
except Exception as e:
    print("SCHEMA ENFORCEMENT -> escritura rechazada:", str(e).split('\n')[0][:200])

# b) Schema Evolution: se habilita explícitamente con mergeSchema
df_prueba.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable(BRONZE)
print("SCHEMA EVOLUTION -> columna 'device_id' agregada a Bronze.")
spark.table(BRONZE).printSchema()

# COMMAND ----------

# Celda 1 (Python) - Ingesta leyendo BRONZE (Evaluación Perezosa)
from pyspark.sql.functions import current_timestamp, lit

df_raw_events = spark.table(BRONZE) \
    .withColumn('silver_load_time', current_timestamp())

print('Lazy Evaluation: plan lógico registrado. 0 lecturas de datos hasta invocar una Acción.')

# COMMAND ----------

# Celda 2 (Python) - Limpieza, Transformación y Filtrado (df_cleaned, df_filtered)
from pyspark.sql.functions import col, upper, trim, round

df_cleaned = df_raw_events \
    .withColumn('category', upper(trim(col('category')))) \
    .withColumn('region', upper(trim(col('region')))) \
    .withColumn('total_amount', round(col('amount') * col('quantity'), 2)) \
    .filter(col('event_id').isNotNull() & (col('amount') > 0)) \
    .dropDuplicates(['event_id'])

df_filtered = df_cleaned.filter(col('event_type') == 'purchase')
print('Transformaciones de limpieza, deduplicación y filtrado agregadas al DAG (todavía sin ejecutar).')

# COMMAND ----------

# Celda 3 (Python) - Funciones de Ventana (WindowSpec)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, avg, col, round

user_window = Window.partitionBy('user_id').orderBy(col('event_timestamp').desc())
region_window = Window.partitionBy('region', 'category')

df_windowed = df_filtered \
    .withColumn('user_purchase_rank', row_number().over(user_window)) \
    .withColumn('region_avg_amount', round(avg('total_amount').over(region_window), 2))
print('WindowSpec registrado en el plan lógico.')

# COMMAND ----------

# Celda 4 (Python) - Join Optimizado con broadcast()
from pyspark.sql.functions import broadcast

regiones_meta = [
    ('NORTE', 'Zona Norte - Sede Principal', 'SDR-NORTE'),
    ('SUR', 'Zona Sur - Sede Operativa', 'SDR-SUR'),
    ('ESTE', 'Zona Este - Sede Logística', 'SDR-ESTE'),
    ('OESTE', 'Zona Oeste - Sede Comercial', 'SDR-OESTE'),
    ('CENTRO', 'Zona Centro - Sede Administrativa', 'SDR-CENTRO')
]
df_regiones = spark.createDataFrame(regiones_meta, ['region', 'region_nombre', 'region_code'])
df_enriched = df_windowed.join(broadcast(df_regiones), on='region', how='inner')
print('Broadcast Join configurado.')

# Evidencia de la Evaluación Perezosa: Catalyst muestra el plan optimizado SIN haber procesado datos
df_enriched.explain(mode='formatted')

# COMMAND ----------

# Celda 4b (Python) - Función reutilizable de la capa Silver (la usa la ingesta incremental del ESP32, celda 9)
def pipeline_silver(df_raw):
    w_user = Window.partitionBy('user_id').orderBy(col('event_timestamp').desc())
    w_region = Window.partitionBy('region', 'category')
    return (df_raw
            .withColumn('silver_load_time', current_timestamp())
            .withColumn('category', upper(trim(col('category'))))
            .withColumn('region', upper(trim(col('region'))))
            .withColumn('total_amount', round(col('amount') * col('quantity'), 2))
            .filter(col('event_id').isNotNull() & (col('amount') > 0))
            .dropDuplicates(['event_id'])
            .filter(col('event_type') == 'purchase')
            .withColumn('user_purchase_rank', row_number().over(w_user))
            .withColumn('region_avg_amount', round(avg('total_amount').over(w_region), 2))
            .join(broadcast(df_regiones), on='region', how='inner'))

print('Función pipeline_silver() lista.')

# COMMAND ----------

# Celda 5 (Python) - Escritura de la Tabla Delta silver_events (ACCIÓN FÍSICA)
import time
t0 = time.time()
df_enriched.write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(SILVER)
print(f'¡Acción ejecutada en {time.time() - t0:.1f}s! Aquí recién se leyó Bronze y se ejecutó todo el DAG.')
print(f'Filas en Silver (solo compras limpias): {spark.table(SILVER).count():,}')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 5b - Calidad de datos: qué limpió Silver respecto a Bronze
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bronze.eventos_raw)                                        AS filas_bronze,
# MAGIC   (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM bronze.eventos_raw)            AS duplicados_bronze,
# MAGIC   (SELECT COUNT(*) FROM bronze.eventos_raw WHERE amount <= 0)                      AS montos_invalidos,
# MAGIC   (SELECT COUNT(*) FROM bronze.eventos_raw WHERE category <> UPPER(TRIM(category))) AS categorias_sucias,
# MAGIC   (SELECT COUNT(*) FROM silver.silver_events)                                      AS compras_limpias_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 6a - Capa Gold 1: gold_sales_summary con LIQUID CLUSTERING (CLUSTER BY) + OPTIMIZE
# MAGIC CREATE OR REPLACE TABLE gold.gold_sales_summary
# MAGIC CLUSTER BY (region, categoria)
# MAGIC AS SELECT
# MAGIC   region,
# MAGIC   region_nombre,
# MAGIC   category                                         AS categoria,
# MAGIC   CAST(DATE_TRUNC('month', event_timestamp) AS DATE) AS mes,
# MAGIC   COUNT(DISTINCT user_id)                          AS usuarios_unicos,
# MAGIC   COUNT(event_id)                                  AS total_compras,
# MAGIC   ROUND(SUM(total_amount), 2)                      AS ingresos_totales,
# MAGIC   ROUND(AVG(total_amount), 2)                      AS ticket_promedio
# MAGIC FROM silver.silver_events
# MAGIC GROUP BY region, region_nombre, category, CAST(DATE_TRUNC('month', event_timestamp) AS DATE);
# MAGIC
# MAGIC -- Con Liquid Clustering, OPTIMIZE reorganiza los archivos según las claves de CLUSTER BY (no se usa ZORDER)
# MAGIC OPTIMIZE gold.gold_sales_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 6b - Capa Gold 2: gold_user_metrics (KPIs por usuario) con LIQUID CLUSTERING
# MAGIC CREATE OR REPLACE TABLE gold.gold_user_metrics
# MAGIC CLUSTER BY (user_id)
# MAGIC AS SELECT
# MAGIC   user_id,
# MAGIC   region,
# MAGIC   COUNT(event_id)             AS total_compras_usuario,
# MAGIC   ROUND(SUM(total_amount), 2) AS gasto_total_usuario,
# MAGIC   ROUND(AVG(total_amount), 2) AS gasto_promedio_usuario,
# MAGIC   MAX(event_timestamp)        AS ultima_compra
# MAGIC FROM silver.silver_events
# MAGIC GROUP BY user_id, region;
# MAGIC
# MAGIC OPTIMIZE gold.gold_user_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 6c - Inspección y verificación de datos en la Capa Gold
# MAGIC SELECT region_nombre, categoria, mes, usuarios_unicos, total_compras, ingresos_totales, ticket_promedio
# MAGIC FROM gold.gold_sales_summary
# MAGIC ORDER BY ingresos_totales DESC
# MAGIC LIMIT 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 6d - Verificar que las tablas Gold usan Liquid Clustering (columna clusteringColumns)
# MAGIC DESCRIBE DETAIL gold.gold_sales_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚡ Encender el ESP32 #1 (productor)
# MAGIC A partir de aquí el ESP32 #1 inserta lotes de eventos en `bronze.eventos_raw` (con `source = 'ESP32'` y `device_id`).
# MAGIC Deja que envíe varios lotes y continúa con la celda 7.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 7 - Auditoría del Delta Log: cada lote del ESP32 aparece como una nueva versión (operación WRITE)
# MAGIC DESCRIBE HISTORY bronze.eventos_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 7b - Eventos recibidos desde el hardware
# MAGIC SELECT device_id, COUNT(*) AS eventos, MIN(ingestion_time) AS primero, MAX(ingestion_time) AS ultimo
# MAGIC FROM bronze.eventos_raw
# MAGIC WHERE source = 'ESP32'
# MAGIC GROUP BY device_id;

# COMMAND ----------

# Celda 9 (Python) - Ingesta incremental del ESP32 hacia Silver con MERGE INTO (UPSERT idempotente)
# Si el ESP32 reenvía un lote (por ejemplo, tras perder Wi-Fi), MERGE evita duplicar compras en Silver.
nuevos = pipeline_silver(spark.table(BRONZE).filter(col('source') == 'ESP32'))
nuevos.createOrReplaceTempView('nuevos_esp32')

resultado = spark.sql(f"""
  MERGE INTO {SILVER} AS t
  USING nuevos_esp32 AS s
  ON t.event_id = s.event_id
  WHEN NOT MATCHED THEN INSERT *
""")
display(resultado)   # num_inserted_rows: compras nuevas del ESP32. Si se ejecuta dos veces, la segunda inserta 0.

# COMMAND ----------

# Celda 9b (Python) - Refrescar la capa Gold con los datos nuevos (mismas consultas de 6a y 6b)
spark.sql("""
CREATE OR REPLACE TABLE gold.gold_sales_summary CLUSTER BY (region, categoria) AS
SELECT region, region_nombre, category AS categoria,
       CAST(DATE_TRUNC('month', event_timestamp) AS DATE) AS mes,
       COUNT(DISTINCT user_id) AS usuarios_unicos, COUNT(event_id) AS total_compras,
       ROUND(SUM(total_amount), 2) AS ingresos_totales, ROUND(AVG(total_amount), 2) AS ticket_promedio
FROM silver.silver_events
GROUP BY region, region_nombre, category, CAST(DATE_TRUNC('month', event_timestamp) AS DATE)
""")
spark.sql("""
CREATE OR REPLACE TABLE gold.gold_user_metrics CLUSTER BY (user_id) AS
SELECT user_id, region, COUNT(event_id) AS total_compras_usuario,
       ROUND(SUM(total_amount), 2) AS gasto_total_usuario, ROUND(AVG(total_amount), 2) AS gasto_promedio_usuario,
       MAX(event_timestamp) AS ultima_compra
FROM silver.silver_events
GROUP BY user_id, region
""")
spark.sql("OPTIMIZE gold.gold_sales_summary")
spark.sql("OPTIMIZE gold.gold_user_metrics")
print("Gold actualizada. El ESP32 #2 mostrará los nuevos totales en su próxima consulta.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 8 - Time Travel: comparar Silver antes y después de la ingesta del ESP32
# MAGIC SELECT 'version 0 (carga inicial)' AS estado, COUNT(*) AS registros FROM silver.silver_events VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'version actual (con ESP32)', COUNT(*) FROM silver.silver_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 10 - Small File Problem: cada lote del ESP32 creó archivos pequeños en Bronze
# MAGIC DESCRIBE DETAIL bronze.eventos_raw;   -- observar numFiles y sizeInBytes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 10b - Compactación con OPTIMIZE (luego vuelve a ejecutar la celda 10 y compara numFiles)
# MAGIC OPTIMIZE bronze.eventos_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 11 - Recuperación ante fallas: simular un job defectuoso en Gold y revertirlo con RESTORE
# MAGIC DELETE FROM gold.gold_sales_summary WHERE region = 'NORTE';      -- "falla" del job
# MAGIC SELECT COUNT(*) AS filas_despues_de_falla FROM gold.gold_sales_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.gold_sales_summary;   -- identificar la versión anterior al DELETE

# COMMAND ----------

# Celda 11b (Python) - RESTORE a la versión inmediatamente anterior al DELETE
hist = spark.sql("DESCRIBE HISTORY gold.gold_sales_summary").orderBy(col('version').desc()).collect()
version_previa = [h for h in hist if h['operation'] != 'DELETE' and h['version'] < hist[0]['version']][0]['version']
spark.sql(f"RESTORE TABLE gold.gold_sales_summary TO VERSION AS OF {version_previa}")
print(f"Tabla restaurada a la versión {version_previa}. Filas:",
      spark.table('gold.gold_sales_summary').count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 12 - VACUUM en modo simulación: lista archivos que se borrarían (retención por defecto 7 días)
# MAGIC VACUUM bronze.eventos_raw DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 13 - Consultas que ejecuta el ESP32 #2 (sirven para validar el dashboard físico)
# MAGIC SELECT CAST(mes AS STRING) AS mes, region, categoria, ROUND(SUM(ingresos_totales), 2) AS ingresos, SUM(total_compras) AS compras
# MAGIC FROM gold.gold_sales_summary
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, 2, 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 14 - Modelo dimensional (estrella) en Gold para Power BI / Databricks SQL
# MAGIC CREATE OR REPLACE TABLE gold.dim_region AS
# MAGIC SELECT DISTINCT region, region_nombre, region_code FROM silver.silver_events;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.dim_categoria AS
# MAGIC SELECT DISTINCT category AS categoria FROM silver.silver_events;
# MAGIC
# MAGIC -- Vista de eventos en vivo del ESP32 (para Power BI y el dashboard)
# MAGIC CREATE OR REPLACE VIEW gold.vw_eventos_esp32 AS
# MAGIC SELECT event_id, device_id, event_type, category AS categoria, region, amount, quantity,
# MAGIC        ROUND(amount * quantity, 2) AS total_amount, ingestion_time
# MAGIC FROM bronze.eventos_raw
# MAGIC WHERE source = 'ESP32';
# MAGIC
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cuestionario de Evaluación Teórica
# MAGIC *Responder con palabras del grupo, apoyándose en lo observado en las celdas indicadas.*
# MAGIC
# MAGIC | # | Pregunta | Evidencia en este notebook |
# MAGIC |---|---|---|
# MAGIC | 1 | Evaluación perezosa, Catalyst y DAG | Celdas 1–4 (sin ejecución) y `explain` en 4; ejecución real en 5 |
# MAGIC | 2 | Beneficio de Bronze/Silver/Gold | Celda 5b (qué se limpió) y consumo desde Gold por el ESP32 #2 |
# MAGIC | 3 | Liquid Clustering vs particionamiento | Celdas 6a, 6b y 6d (`clusteringColumns`) |
# MAGIC | 4 | ACID y `_delta_log` | Celda 7: Spark y ESP32 escribiendo en la misma tabla |
# MAGIC | 5 | Schema enforcement / evolution | Celda 0b (`mergeSchema`, columna `device_id`) |
# MAGIC | 6 | Time Travel y auditoría | Celdas 7, 8, 11 y 11b (`RESTORE`) |
# MAGIC | 7 | Narrow vs wide transformations | `filter`/`withColumn` (2) vs ventanas, `dropDuplicates`, `groupBy` (3, 6a) |
# MAGIC | 8 | Data skew, salting y AQE | Pensar qué pasaría si un solo `device_id` o región concentrara casi todo |
# MAGIC | 9 | Small file problem y OPTIMIZE | Celdas 10 y 10b (`numFiles` antes y después) |
# MAGIC | 10 | VACUUM y retención de 7 días | Celda 12 (`DRY RUN`) |
# MAGIC | 11 | MERGE vs APPEND | Celda 9 (ejecutarla dos veces: la segunda inserta 0 filas) |
# MAGIC
# MAGIC **Respuesta 1:**
# MAGIC
# MAGIC **Respuesta 2:**
# MAGIC
# MAGIC **Respuesta 3:**
# MAGIC
# MAGIC **Respuesta 4:**
# MAGIC
# MAGIC **Respuesta 5:**
# MAGIC
# MAGIC **Respuesta 6:**
# MAGIC
# MAGIC **Respuesta 7:**
# MAGIC
# MAGIC **Respuesta 8:**
# MAGIC
# MAGIC **Respuesta 9:**
# MAGIC
# MAGIC **Respuesta 10:**
# MAGIC
# MAGIC **Respuesta 11:**
