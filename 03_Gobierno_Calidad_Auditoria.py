# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gobierno, calidad y auditoría de datos
# MAGIC
# MAGIC Capa de gobierno sobre el Lakehouse Medallón. **No modifica** Bronze, Silver ni Gold: crea el esquema `gobierno` y vistas nuevas.
# MAGIC
# MAGIC | Componente | Referencia | Qué se implementa |
# MAGIC |---|---|---|
# MAGIC | Gobierno | ISO/IEC 38505-1 | Catálogo de datos (propietario, custodio, clasificación, finalidad, retención), comentarios y etiquetas en Unity Catalog |
# MAGIC | Calidad | ISO 8000 | Reglas medibles por dimensión (completitud, unicidad, validez, consistencia, oportunidad) con umbral por capa y resultados históricos |
# MAGIC | Control de acceso | RBAC | `GRANT` por grupo con mínimo privilegio |
# MAGIC | Control de acceso | ABAC | Vistas gobernadas: filtro de filas por región y máscara de columna según atributos del usuario |
# MAGIC | Auditoría | — | Historial Delta conservado en una tabla + `system.access.audit` si la cuenta lo permite |
# MAGIC
# MAGIC > Este notebook **alinea** el proyecto con los conceptos de las normas; no equivale a una certificación.
# MAGIC Requisito: haber ejecutado el notebook 01 (existen Bronze, Silver y Gold). Es idempotente: se puede ejecutar varias veces.

# COMMAND ----------

# Celda 1 - Configuración
import uuid, datetime
from pyspark.sql import functions as F

CATALOG = "proyecto_nota3"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gobierno")

BRONZE = f"{CATALOG}.bronze.eventos_raw"
SILVER = f"{CATALOG}.silver.silver_events"
GOLD_VENTAS = f"{CATALOG}.gold.gold_sales_summary"
GOLD_USUARIOS = f"{CATALOG}.gold.gold_user_metrics"

USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Catálogo: {CATALOG} | usuario que ejecuta: {USUARIO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Gobierno de datos (ISO/IEC 38505-1)
# MAGIC La norma pide **rendición de cuentas** (accountability): cada conjunto de datos debe tener un responsable, una clasificación y una finalidad declarada.
# MAGIC Los valores de propietario, custodio, clasificación y retención son una **propuesta de ejemplo**; ajústalos a lo que defina el grupo.

# COMMAND ----------

# Celda 2 - Catálogo de datos: quién es responsable de qué
esquema = "tabla STRING, capa STRING, dominio STRING, propietario STRING, custodio STRING, clasificacion STRING, dato_personal STRING, finalidad STRING, retencion_dias INT"
catalogo = [
    ("bronze.eventos_raw", "Bronze", "Ventas IoT", USUARIO, "Equipo de datos", "Interna", "Seudónimo (user_id)", "Ingesta cruda de eventos (Spark y ESP32)", 365),
    ("silver.silver_events", "Silver", "Ventas IoT", USUARIO, "Equipo de datos", "Interna", "Seudónimo (user_id)", "Compras limpias y deduplicadas", 365),
    ("gold.gold_sales_summary", "Gold", "Ventas IoT", USUARIO, "Equipo de datos", "Confidencial", "No", "KPIs por región, categoría y mes", 730),
    ("gold.gold_user_metrics", "Gold", "Ventas IoT", USUARIO, "Equipo de datos", "Confidencial", "Seudónimo (user_id)", "KPIs por usuario", 730),
    ("gold.vw_ventas_gobernada", "Gold", "Ventas IoT", USUARIO, "Equipo de datos", "Confidencial", "No", "Acceso a KPIs con filtro por región (ABAC)", 730),
    ("gold.vw_usuarios_gobernada", "Gold", "Ventas IoT", USUARIO, "Equipo de datos", "Confidencial", "Seudonimizado", "Acceso a KPIs por usuario con máscara (ABAC)", 730),
]
(spark.createDataFrame(catalogo, esquema)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.gobierno.gov_catalogo_datos"))
display(spark.table(f"{CATALOG}.gobierno.gov_catalogo_datos"))

# COMMAND ----------

# Celda 3 - Comentarios y etiquetas en Unity Catalog (metadatos visibles en el Catalog Explorer)
etiquetas = {
    "bronze.eventos_raw":      ("Bronze: eventos crudos de Spark y ESP32", "Interna"),
    "silver.silver_events":    ("Silver: compras limpias, deduplicadas y enriquecidas", "Interna"),
    "gold.gold_sales_summary": ("Gold: KPIs de ventas por región, categoría y mes", "Confidencial"),
    "gold.gold_user_metrics":  ("Gold: KPIs por usuario (user_id seudónimo)", "Confidencial"),
}
for tabla, (comentario, clasif) in etiquetas.items():
    try:
        spark.sql(f"COMMENT ON TABLE {CATALOG}.{tabla} IS '{comentario}'")
        spark.sql(f"ALTER TABLE {CATALOG}.{tabla} SET TAGS ('clasificacion' = '{clasif}', 'norma' = 'ISO-38505')")
        print(f"OK   {tabla}  -> {clasif}")
    except Exception as e:
        print(f"OMITIDO {tabla}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Control de acceso: ABAC (atributos) y RBAC (roles)
# MAGIC **ABAC.** La tabla `gobierno.gov_roles_acceso` guarda los atributos de cada usuario (rol, región permitida, nivel de acceso a datos personales).
# MAGIC Las vistas gobernadas leen `current_user()` y deciden qué filas y columnas devolver. Los consumidores usan las vistas, no las tablas base.
# MAGIC
# MAGIC Los usuarios `analista.norte@ejemplo.com` y `gerencia@ejemplo.com` son **ejemplos ficticios** para demostrar la política.

# COMMAND ----------

# Celda 4 - Atributos de acceso por usuario (fuente de la política ABAC)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gobierno.gov_roles_acceso (
  usuario STRING, rol STRING, region_permitida STRING, nivel_pii STRING,
  vigente BOOLEAN, otorgado_por STRING, otorgado_en TIMESTAMP)
""")
spark.sql(f"""
MERGE INTO {CATALOG}.gobierno.gov_roles_acceso t
USING (SELECT * FROM VALUES
  ('{USUARIO}',                 'ADMIN',    '*',     'FULL',   true, '{USUARIO}', current_timestamp()),
  ('analista.norte@ejemplo.com','ANALISTA', 'NORTE', 'MASKED', true, '{USUARIO}', current_timestamp()),
  ('gerencia@ejemplo.com',      'GERENCIA', '*',     'MASKED', true, '{USUARIO}', current_timestamp())
  AS v(usuario, rol, region_permitida, nivel_pii, vigente, otorgado_por, otorgado_en)) s
ON t.usuario = s.usuario
WHEN NOT MATCHED THEN INSERT *
""")
display(spark.table(f"{CATALOG}.gobierno.gov_roles_acceso"))

# COMMAND ----------

# Celda 5 - Vistas gobernadas (filtro de filas por región + máscara de user_id)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.vw_ventas_gobernada AS
SELECT s.*
FROM {GOLD_VENTAS} s
WHERE EXISTS (
  SELECT 1 FROM {CATALOG}.gobierno.gov_roles_acceso a
  WHERE a.usuario = current_user() AND a.vigente
    AND (a.region_permitida = '*' OR a.region_permitida = s.region))
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.vw_usuarios_gobernada AS
SELECT
  CASE WHEN (SELECT MAX(CASE WHEN nivel_pii = 'FULL' THEN 1 ELSE 0 END)
             FROM {CATALOG}.gobierno.gov_roles_acceso
             WHERE usuario = current_user() AND vigente) = 1
       THEN u.user_id
       ELSE CONCAT('USR-', SUBSTR(SHA2(u.user_id, 256), 1, 8)) END AS user_id,
  u.region, u.total_compras_usuario, u.gasto_total_usuario,
  u.gasto_promedio_usuario, u.ultima_compra
FROM {GOLD_USUARIOS} u
WHERE EXISTS (
  SELECT 1 FROM {CATALOG}.gobierno.gov_roles_acceso a
  WHERE a.usuario = current_user() AND a.vigente
    AND (a.region_permitida = '*' OR a.region_permitida = u.region))
""")
print("Vistas gobernadas creadas.")

# COMMAND ----------

# Celda 6 - Prueba de la política: lo que ve el usuario actual y lo que verían otros perfiles
print(f"Vista del usuario actual ({USUARIO}):")
display(spark.sql(f"SELECT region, COUNT(*) AS filas FROM {CATALOG}.gold.vw_ventas_gobernada GROUP BY region ORDER BY region"))

# Simulación con un usuario literal (la misma lógica de la vista, sin necesitar iniciar sesión como ese usuario)
for correo in ["analista.norte@ejemplo.com", "gerencia@ejemplo.com", "desconocido@ejemplo.com"]:
    df = spark.sql(f"""
      SELECT '{correo}' AS usuario, s.region, COUNT(*) AS filas
      FROM {GOLD_VENTAS} s
      WHERE EXISTS (SELECT 1 FROM {CATALOG}.gobierno.gov_roles_acceso a
                    WHERE a.usuario = '{correo}' AND a.vigente
                      AND (a.region_permitida = '*' OR a.region_permitida = s.region))
      GROUP BY s.region ORDER BY s.region""")
    print(f"\nRegiones visibles para {correo}:", [r['region'] for r in df.collect()] or "ninguna (acceso denegado)")

# COMMAND ----------

# Celda 7 - RBAC: permisos por rol (mínimo privilegio) sobre grupos de Unity Catalog
# Los grupos se administran a nivel de cuenta. Si no existen (por ejemplo, en Free Edition), cada GRANT se omite y se informa.
C = CATALOG
PERMISOS = {
    "grp_ingesta_iot":      [f"USE CATALOG ON CATALOG {C}", f"USE SCHEMA ON SCHEMA {C}.bronze",
                             f"SELECT ON TABLE {BRONZE}", f"MODIFY ON TABLE {BRONZE}"],
    "grp_ingenieria_datos": [f"USE CATALOG ON CATALOG {C}", f"ALL PRIVILEGES ON SCHEMA {C}.bronze",
                             f"ALL PRIVILEGES ON SCHEMA {C}.silver", f"ALL PRIVILEGES ON SCHEMA {C}.gold"],
    "grp_analistas":        [f"USE CATALOG ON CATALOG {C}", f"USE SCHEMA ON SCHEMA {C}.gold",
                             f"SELECT ON TABLE {C}.gold.vw_ventas_gobernada",
                             f"SELECT ON TABLE {C}.gold.vw_usuarios_gobernada",
                             f"SELECT ON TABLE {C}.gold.dim_region", f"SELECT ON TABLE {C}.gold.dim_categoria"],
    "grp_gerencia":         [f"USE CATALOG ON CATALOG {C}", f"USE SCHEMA ON SCHEMA {C}.gold",
                             f"SELECT ON TABLE {C}.gold.vw_ventas_gobernada",
                             f"SELECT ON TABLE {C}.gold.vw_usuarios_gobernada"],
    "grp_auditoria":        [f"USE CATALOG ON CATALOG {C}", f"USE SCHEMA ON SCHEMA {C}.gobierno",
                             f"SELECT ON SCHEMA {C}.gobierno"],
}
aplicados, omitidos = 0, 0
for grupo, permisos in PERMISOS.items():
    for p in permisos:
        try:
            spark.sql(f"GRANT {p} TO `{grupo}`")
            aplicados += 1
        except Exception as e:
            omitidos += 1
            if omitidos <= 3:
                print(f"OMITIDO ({grupo}): {str(e).splitlines()[0][:140]}")
print(f"RBAC -> {aplicados} permisos aplicados, {omitidos} omitidos (grupo inexistente o sin privilegios para otorgar).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Calidad de datos (ISO 8000)
# MAGIC Cada regla mide una **dimensión de calidad** y se compara con un **umbral por capa**: Bronze tolera defectos (es el dato crudo), Silver y Gold exigen 100 %.
# MAGIC Cada ejecución guarda un histórico en `gobierno.dq_resultados`, así se ve cómo Silver corrige lo que Bronze trae.

# COMMAND ----------

# Celda 8 - Reglas de calidad y ejecución
def regla_filas(tabla, condicion_falla):
    return (f"SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN {condicion_falla} THEN 1 ELSE 0 END), 0) AS fallas FROM {tabla}")

def regla_unicidad(tabla, columna):
    return f"SELECT COUNT(*) AS total, COUNT(*) - COUNT(DISTINCT {columna}) AS fallas FROM {tabla}"

REGIONES_OK = "('NORTE','SUR','ESTE','OESTE','CENTRO')"
CATEGORIAS_OK = "('ELECTRONICA','HOGAR','ROPA','DEPORTES','JUGUETERIA')"

# (capa, tabla, regla, dimensión, umbral %, SQL que devuelve total y fallas)
REGLAS = [
    ("Bronze", BRONZE, "event_id no nulo", "Completitud", 100, regla_filas(BRONZE, "event_id IS NULL")),
    ("Bronze", BRONZE, "user_id, region y category no nulos", "Completitud", 100,
        regla_filas(BRONZE, "user_id IS NULL OR region IS NULL OR category IS NULL")),
    ("Bronze", BRONZE, "event_id único", "Unicidad", 98, regla_unicidad(BRONZE, "event_id")),
    ("Bronze", BRONZE, "amount > 0", "Validez", 99, regla_filas(BRONZE, "amount IS NULL OR amount <= 0")),
    ("Bronze", BRONZE, "category normalizada (mayúsculas, sin espacios)", "Validez", 97,
        regla_filas(BRONZE, "category <> UPPER(TRIM(category))")),
    ("Bronze", BRONZE, "region dentro del dominio válido", "Validez", 99,
        regla_filas(BRONZE, f"UPPER(TRIM(region)) NOT IN {REGIONES_OK}")),
    ("Bronze", BRONZE, "ESP32 envió eventos en los últimos 60 min", "Oportunidad", 100,
        f"SELECT 1 AS total, CASE WHEN MAX(ingestion_time) IS NULL OR MAX(ingestion_time) < current_timestamp() - INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS fallas FROM {BRONZE} WHERE source = 'ESP32'"),
    ("Silver", SILVER, "event_id único", "Unicidad", 100, regla_unicidad(SILVER, "event_id")),
    ("Silver", SILVER, "amount > 0", "Validez", 100, regla_filas(SILVER, "amount IS NULL OR amount <= 0")),
    ("Silver", SILVER, "category dentro del dominio válido", "Validez", 100,
        regla_filas(SILVER, f"category NOT IN {CATEGORIAS_OK}")),
    ("Silver", SILVER, "region dentro del dominio válido", "Validez", 100,
        regla_filas(SILVER, f"region NOT IN {REGIONES_OK}")),
    ("Silver", SILVER, "total_amount = amount x quantity", "Consistencia", 100,
        regla_filas(SILVER, "ABS(total_amount - ROUND(amount * quantity, 2)) > 0.01")),
    ("Silver", SILVER, "region_nombre informado", "Completitud", 100, regla_filas(SILVER, "region_nombre IS NULL")),
    ("Gold", GOLD_VENTAS, "total_compras (Gold) = filas de Silver", "Consistencia", 100,
        f"SELECT 1 AS total, CASE WHEN (SELECT SUM(total_compras) FROM {GOLD_VENTAS}) = (SELECT COUNT(*) FROM {SILVER}) THEN 0 ELSE 1 END AS fallas"),
    ("Gold", GOLD_USUARIOS, "suma por usuario (Gold) = filas de Silver", "Consistencia", 100,
        f"SELECT 1 AS total, CASE WHEN (SELECT SUM(total_compras_usuario) FROM {GOLD_USUARIOS}) = (SELECT COUNT(*) FROM {SILVER}) THEN 0 ELSE 1 END AS fallas"),
]

RUN_ID = str(uuid.uuid4())[:8]
AHORA = datetime.datetime.now()
filas = []
for capa, tabla, regla, dimension, umbral, sql in REGLAS:
    r = spark.sql(sql).collect()[0]
    total, fallas = int(r["total"]), int(r["fallas"])
    pct = round(100.0 * (total - fallas) / total, 3) if total else 100.0
    filas.append((RUN_ID, AHORA, capa, tabla.split(".", 1)[1], regla, dimension, total, fallas, pct, float(umbral),
                  "OK" if pct >= umbral else "ALERTA"))

esquema_dq = ("run_id STRING, ejecutado_en TIMESTAMP, capa STRING, tabla STRING, regla STRING, dimension STRING, "
              "filas_evaluadas LONG, filas_con_falla LONG, pct_cumplimiento DOUBLE, umbral_pct DOUBLE, estado STRING")
spark.createDataFrame(filas, esquema_dq).write.mode("append").saveAsTable(f"{CATALOG}.gobierno.dq_resultados")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gobierno.vw_calidad_ultima AS
SELECT * FROM {CATALOG}.gobierno.dq_resultados
WHERE run_id = (SELECT run_id FROM {CATALOG}.gobierno.dq_resultados ORDER BY ejecutado_en DESC LIMIT 1)
""")
print(f"Ejecución de calidad {RUN_ID}: {sum(1 for f in filas if f[-1] == 'OK')} reglas OK, {sum(1 for f in filas if f[-1] == 'ALERTA')} en ALERTA")
display(spark.sql(f"SELECT capa, dimension, regla, filas_evaluadas, filas_con_falla, pct_cumplimiento, umbral_pct, estado FROM {CATALOG}.gobierno.vw_calidad_ultima ORDER BY capa DESC, dimension"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Auditoría
# MAGIC 1. **Historial Delta:** cada cambio de tabla (quién, cuándo, qué operación). El log de Delta se limpia con el tiempo, así que se copia a una tabla propia para conservarlo.
# MAGIC 2. **`system.access.audit`:** registro de la plataforma (quién accedió a qué), si la cuenta tiene habilitadas las tablas de sistema.

# COMMAND ----------

# Celda 9 - Conservar el historial Delta en gobierno.audit_historial_delta (solo agrega versiones nuevas)
TABLAS_AUDITADAS = [BRONZE, SILVER, GOLD_VENTAS, GOLD_USUARIOS, f"{CATALOG}.gobierno.gov_roles_acceso"]
AUD = f"{CATALOG}.gobierno.audit_historial_delta"

partes = []
for t in TABLAS_AUDITADAS:
    partes.append(spark.sql(f"DESCRIBE HISTORY {t}").select(
        F.lit(t.split(".", 1)[1]).alias("tabla"), "version", "timestamp", "userName", "operation",
        F.to_json("operationMetrics").alias("metricas")))
nuevos = partes[0]
for p in partes[1:]:
    nuevos = nuevos.unionByName(p)

if spark.catalog.tableExists(AUD):
    nuevos = nuevos.join(spark.table(AUD).select("tabla", "version"), ["tabla", "version"], "left_anti")
nuevos.write.mode("append").saveAsTable(AUD)
print(f"Versiones nuevas registradas: {nuevos.count()} | total en auditoría: {spark.table(AUD).count()}")
display(spark.sql(f"SELECT tabla, version, timestamp, userName, operation FROM {AUD} ORDER BY timestamp DESC LIMIT 20"))

# COMMAND ----------

# Celda 10 - Registro de auditoría de la plataforma (system.access.audit), si está disponible
try:
    display(spark.sql("""
        SELECT event_time, user_identity.email AS usuario, service_name, action_name
        FROM system.access.audit
        WHERE event_date >= current_date() - INTERVAL 1 DAYS
        ORDER BY event_time DESC LIMIT 20"""))
except Exception as e:
    print("system.access.audit no disponible en esta cuenta:", str(e).splitlines()[0][:160])
    print("Se usa el historial Delta (celda 9) como evidencia de auditoría.")

# COMMAND ----------

# Celda 11 - Permisos vigentes (evidencia del control de acceso)
for objeto in [f"SCHEMA {CATALOG}.gold", f"TABLE {CATALOG}.gold.vw_ventas_gobernada"]:
    try:
        print(f"\nSHOW GRANTS ON {objeto}")
        display(spark.sql(f"SHOW GRANTS ON {objeto}"))
    except Exception as e:
        print("No se pudo consultar:", str(e).splitlines()[0][:140])
