# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Refresco automático Bronze → Silver → Gold
# MAGIC
# MAGIC **Para la demo:** ejecuta este notebook con **Run all** y déjalo corriendo. No hay que tocar nada más.
# MAGIC
# MAGIC 1. El **ESP32 #1** inserta lotes en `bronze.eventos_raw`.
# MAGIC 2. Este notebook detecta cada versión nueva de Bronze (Delta log), hace el `MERGE INTO` hacia Silver y recalcula Gold.
# MAGIC 3. El **ESP32 #2** consulta Gold y muestra los números nuevos en su siguiente ciclo (30 s).
# MAGIC
# MAGIC Requisito: haber ejecutado antes las celdas 00 → 6d de `01_Lakehouse_Medallon_ESP32.py` (existen Bronze, Silver y Gold).
# MAGIC Para detenerlo, pulsa **Interrupt** o cambia el widget `detener` a `si`. Con `max_minutos` > 0 se detiene solo (útil para pruebas); con `0` corre sin límite.

# COMMAND ----------

# Celda 1 - Configuración
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, upper, trim, round, avg, row_number,
                                   current_timestamp, broadcast)

dbutils.widgets.text("intervalo_seg", "10", "Revisar Bronze cada (seg)")
dbutils.widgets.dropdown("detener", "no", ["no", "si"], "Detener")
dbutils.widgets.text("max_minutos", "0", "Duración máxima en min (0 = sin límite)")
INTERVALO = int(dbutils.widgets.get("intervalo_seg"))
MAX_SEG = int(dbutils.widgets.get("max_minutos")) * 60

CATALOG = "proyecto_nota3"
try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception:
    CATALOG = "workspace"          # mismo criterio que la celda 00 del notebook 01
    spark.sql(f"USE CATALOG {CATALOG}")

BRONZE = f"{CATALOG}.bronze.eventos_raw"
SILVER = f"{CATALOG}.silver.silver_events"
print(f"Catálogo: {CATALOG} | revisando Bronze cada {INTERVALO}s")

# COMMAND ----------

# Celda 2 - Lógica de Silver (misma que pipeline_silver del notebook 01)
regiones_meta = [
    ('NORTE', 'Zona Norte - Sede Principal', 'SDR-NORTE'),
    ('SUR', 'Zona Sur - Sede Operativa', 'SDR-SUR'),
    ('ESTE', 'Zona Este - Sede Logística', 'SDR-ESTE'),
    ('OESTE', 'Zona Oeste - Sede Comercial', 'SDR-OESTE'),
    ('CENTRO', 'Zona Centro - Sede Administrativa', 'SDR-CENTRO'),
]
df_regiones = spark.createDataFrame(regiones_meta, ['region', 'region_nombre', 'region_code'])

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

# COMMAND ----------

# Celda 3 - Un ciclo de refresco: MERGE a Silver y recálculo de Gold
def version_bronze():
    return spark.sql(f"DESCRIBE HISTORY {BRONZE} LIMIT 1").collect()[0]["version"]

def refrescar():
    # Silver: solo inserta compras nuevas del ESP32 (idempotente, no duplica nada)
    pipeline_silver(spark.table(BRONZE).filter(col('source') == 'ESP32')) \
        .createOrReplaceTempView('nuevos_esp32')
    spark.sql(f"""
        MERGE INTO {SILVER} AS t
        USING nuevos_esp32 AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    # Gold: los dos KPIs que lee el ESP32 #2
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
               ROUND(SUM(total_amount), 2) AS gasto_total_usuario,
               ROUND(AVG(total_amount), 2) AS gasto_promedio_usuario,
               MAX(event_timestamp) AS ultima_compra
        FROM silver.silver_events
        GROUP BY user_id, region
    """)

# COMMAND ----------

# Celda 4 - Bucle automático: se queda vigilando Bronze hasta que lo detengas
ultima = -1
ciclos = 0
inicio = time.time()
while dbutils.widgets.get("detener") == "no" and (MAX_SEG == 0 or time.time() - inicio < MAX_SEG):
    try:
        actual = version_bronze()
        if actual != ultima:                       # hay datos nuevos (o compactación) en Bronze
            t0 = time.time()
            refrescar()
            ciclos += 1
            print(f"[{time.strftime('%H:%M:%S')}] Bronze v{actual} -> Silver/Gold actualizados en {time.time()-t0:.1f}s (ciclo {ciclos})")
            if ciclos % 20 == 0:                    # mantenimiento: evita el small file problem
                spark.sql(f"OPTIMIZE {BRONZE}")
                actual = version_bronze()           # OPTIMIZE crea una versión; no la reprocesamos
            ultima = actual
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Error en el ciclo, se reintenta: {str(e)[:200]}")
    time.sleep(INTERVALO)

print("Refresco automático detenido.")
