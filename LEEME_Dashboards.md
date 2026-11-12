# Dashboards: Databricks SQL + Power BI + ESP32

Los tres dashboards leen las **mismas tablas Gold**, así que siempre muestran las mismas cifras:

| Dashboard | Dónde se ve | Para qué sirve en la sustentación |
|---|---|---|
| Databricks SQL (AI/BI), 3 páginas | En el workspace, publicado | Requisito de la rúbrica (30 pts) |
| Power BI, 3 páginas | Power BI Desktop | Modelo estrella, DAX avanzado y BI empresarial |
| Centro de mando ESP32 #2 | Celular o PC en la misma red | El hardware cerrando el ciclo en vivo |

**Requisito previo:** haber ejecutado el notebook hasta la **celda 14**, que crea `dim_region`, `dim_categoria` y `vw_eventos_esp32`.

---

## A. Dashboard en Databricks SQL (3 páginas)

### Importar
1. Elige el archivo de la carpeta `dashboards` que corresponde a tu catálogo (te lo indica la celda 00 del notebook):
   - `Dashboard_Ventas_IoT_proyecto_nota3.lvdash.json`
   - `Dashboard_Ventas_IoT_workspace.lvdash.json`
2. En Databricks, ve a **Workspace**, abre tu carpeta y entra en **⋮ → Import**. Elige **File** y arrastra el archivo.
3. Abre el dashboard, selecciona el **Serverless Starter Warehouse** y pulsa **Publish**.

### Qué contiene
| Página | Contenido |
|---|---|
| **1 · Resumen ejecutivo** | Filtros de región y categoría · 5 KPIs (ingresos, órdenes, ticket promedio, clientes únicos, gasto medio por cliente) · barras región × categoría · torta de participación por región · líneas de evolución mensual · mapa de calor región × categoría · ticket promedio por categoría · tabla de clientes |
| **2 · IoT en vivo (ESP32)** | Filtros de región y dispositivo · KPIs (eventos, dispositivos, ventas IoT, % de eventos que son compra) · ritmo de eventos por minuto en las últimas 2 h · eventos por tipo · tabla con los últimos eventos en hora de Bogotá |
| **3 · Calidad del Lakehouse** | Duplicados, montos inválidos, categorías sin normalizar y filas IoT · volumen por capa Bronze → Silver → Gold · composición de Bronze por origen (Spark vs ESP32) |

La página 3 es la mejor evidencia para la pregunta 2 del cuestionario (para qué sirve cada capa del medallón).

### Retoques recomendados después de importar (5 min)
- **Barras región × categoría:** selecciónalas y ve a **Layout → Group** para que queden agrupadas.
- **Tablas:** haz clic en el encabezado *Gasto total* (o *Hora*) para ordenar de forma descendente.
- Ve a **Settings → Theme** y elige un tema oscuro o claro, según el proyector.
- **Schedule:** programa un refresco cada 10 minutos durante la demo.

> Si algún widget sale con error al importar (Databricks cambia a veces el formato), bórralo y recréalo con **Add visualization**. Los datasets siguen funcionando, así que solo tienes que escoger el tipo de gráfico y los campos.

---

## B. Power BI Desktop (3 páginas)

### 1. Conectar
1. Abre Power BI Desktop y ve a **Obtener datos → Azure Databricks** (en algunas versiones aparece como **Databricks**).
2. Completa la conexión:
   - **Server hostname:** `dbc-2e9d45b0-5357.cloud.databricks.com`
   - **HTTP path:** `/sql/1.0/warehouses/<ID>`
   - **Modo:** **Import**. Si quieres datos en vivo durante la demo, usa **DirectQuery**.
3. **Autenticación:** elige **Personal Access Token** y pega tu token.
4. Marca estas tablas:
   - `gold_sales_summary`
   - `gold_user_metrics`
   - `dim_region`
   - `dim_categoria`
   - `vw_eventos_esp32`

*Atajo:* edita el ID del warehouse en `powerbi/Conexion_Databricks.pbids` y ábrelo con doble clic.

### 2. Modelo estrella
1. Crea la tabla **Calendario** (primer bloque de `powerbi/Medidas_DAX.txt`).
2. Márcala como tabla de fechas: **Herramientas de tabla → Marcar como tabla de fechas**.
3. Crea una tabla vacía **`_Medidas`** donde vivirán todas las medidas.
4. Crea las relaciones, todas de uno a varios con filtro en una sola dirección:

| Dimensión (1) | Hechos (*) |
|---|---|
| `Calendario[Date]` | `gold_sales_summary[mes]` |
| `dim_region[region]` | `gold_sales_summary[region]`, `gold_user_metrics[region]`, `vw_eventos_esp32[region]` |
| `dim_categoria[categoria]` | `gold_sales_summary[categoria]`, `vw_eventos_esp32[categoria]` |

5. **Oculta** las columnas clave de las tablas de hechos (clic derecho → *Ocultar en la vista de informe*). Así los usuarios filtran siempre por las dimensiones.
6. Toma una captura de la vista de **Modelo** para el informe.

### 3. Medidas y tema
- Pega las medidas de `Medidas_DAX.txt`. Incluyen KPIs, variación mes a mes, acumulado del año (YTD), promedio móvil, rankings, textos dinámicos y estado de la ingesta IoT.
- Aplica el tema en **Vista → Temas → Buscar temas** y elige `Tema_Lakehouse_IoT.json`.
- Formatos sugeridos:
  - `Ingresos*` → moneda, 0 decimales
  - `Ticket Promedio` → moneda, 2 decimales
  - `%` → porcentaje, 1 decimal

### 4. Página 1 · "Resumen ejecutivo"
| Zona | Visual | Campos / detalle pro |
|---|---|---|
| Encabezado | Cuadro de texto + **Tarjeta** con `Titulo Dinamico` | El título cambia según los filtros |
| Fila de KPIs | 4 visuales **Tarjeta (nuevo)** | `Ingresos Totales`, `Total Compras`, `Ticket Promedio`, `Clientes Unicos`; en *Referencia/Etiqueta de referencia* agrega `Flecha Variacion` con color fx = `Color Variacion` |
| Lateral | **Segmentaciones** en estilo *mosaico* | `dim_region[region_nombre]`, `dim_categoria[categoria]`, `Calendario[Mes]` |
| Centro | **Columnas agrupadas** | Eje `dim_region[region]` · Leyenda `dim_categoria[categoria]` · Valor `Ingresos Totales` |
| Centro | **Gráfico combinado** (líneas y columnas) | Eje `Calendario[Mes]` · Columnas `Ingresos Totales` · Línea `Promedio Movil 3 Meses` |
| Derecha | **Anillo** | Leyenda `dim_region[region]` · Valor `Ingresos Totales` (etiquetas: categoría y % del total) |
| Pie de página | **Tarjeta** con `Insight Automatico` | Una frase que se reescribe sola según los filtros |

### 5. Página 2 · "Análisis por región y categoría"
- **Matriz:** en filas `dim_region[region_nombre]`, en columnas `dim_categoria[categoria]`, valores `Ingresos Totales`. Activa **Formato condicional → Color de fondo** (escala azul) y tendrás un mapa de calor.
- **Barras horizontales:** `dim_categoria[categoria]` con `Ingresos Totales`, ordenadas de mayor a menor. Agrega `Ranking Categoria` en la información sobre herramientas.
- **Tabla de clientes:** `gold_user_metrics` con barras de datos en `gasto_total_usuario`. Usa un filtro *Top N = 20* por `gasto_total_usuario`.
- **Página de tooltip:** crea una página pequeña (tamaño *Información sobre herramientas*) con un minigráfico de líneas mensual. Asígnala como tooltip del gráfico de columnas de la página 1; al pasar el mouse por una región verás su tendencia.
- **Obtención de detalles (drill-through):** en esta página agrega `dim_region[region_nombre]` en *Obtener detalles*. Desde la página 1, clic derecho sobre una región → *Obtener detalles*.

### 6. Página 3 · "IoT en vivo"
| Visual | Campos |
|---|---|
| Tarjetas | `Estado Ingesta`, `Eventos ESP32`, `Dispositivos Activos`, `Ventas IoT`, `% Conversion IoT` |
| Líneas | Eje `vw_eventos_esp32[ingestion_time]` (jerarquía hasta minuto) · Valor `Eventos ESP32` |
| Columnas | Eje `event_type` · Valor `Eventos ESP32` |
| Tabla | ingestion_time, device_id, event_type, categoria, region, total_amount (orden descendente) |

En modo **DirectQuery** activa **Actualización automática de página** (*Formato de página → Actualizar página*, cada 30 s). Así la página se mueve sola mientras el ESP32 #1 envía datos.

### 7. Toques finales
- **Marcadores + botones:** crea un menú de navegación entre las 3 páginas (*Insertar → Botones → Navegador de páginas*).
- **Ocultar páginas auxiliares:** oculta la página de tooltip.
- **Diseño:** fondo gris claro (`#F3F6FB`), tarjetas blancas con bordes redondeados (radio 10) y sombra suave; esto se configura en *Formato → General → Efectos*.

---

## C. Centro de mando del ESP32 #2
Abre `http://dashboard-esp32.local` o la IP del Monitor Serie. Incluye:
- **Encabezado:** indicador **EN VIVO** y el flujo ESP32 #1 → Bronze → Silver → Gold → ESP32 #2, con semáforo por etapa.
- **5 KPIs:** cada uno con mini-gráfica y variación frente al mes anterior. El mes en curso, que va incompleto, no entra en la comparación.
- **Gráficos:** barras región × categoría, anillo de participación, líneas mensuales (el mes en curso va punteado), mapa de calor y ranking de categorías. Todos muestran el valor exacto al pasar el mouse.
- **Filtros y exportación:** filtros tipo chip por región y categoría, y botón **Exportar CSV** con los datos filtrados.
- **Transmisión en vivo:** los últimos eventos del ESP32 #1; las filas nuevas se resaltan al llegar.
- **Salud del sistema:** señal Wi-Fi, tiempo encendido, memoria libre, latencia hacia Databricks y conteo de consultas OK/error.
- **Tema claro/oscuro**, que se recuerda entre visitas. Para el proyector usa el claro.

**Prueba de consistencia:** con el mismo filtro (por ejemplo, región NORTE), los ingresos deben coincidir en los tres dashboards. Eso demuestra que **Gold es la única fuente de verdad**.
