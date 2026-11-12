# Guía de montaje – Lakehouse Medallón + ESP32

```
ESP32 #1 (genera ventas) ──HTTPS──► Bronze ─► Silver ─► Gold ──HTTPS──► ESP32 #2 (página web con gráficas)
                    Spark (200.000 eventos) ──┘
```

## Archivos
| Archivo | Para qué sirve |
|---|---|
| `01_Lakehouse_Medallon_ESP32.py` | Notebook de Databricks (se importa tal cual) |
| `ESP32_1_Productor/ESP32_1_Productor.ino` | ESP32 que inserta eventos en Bronze |
| `ESP32_2_Dashboard/ESP32_2_Dashboard.ino` | ESP32 que consulta Gold y sirve el dashboard web |
| `vista_previa_dashboard.png` | Así se ve la página del ESP32 #2 (con datos de prueba) |

---

## 1. Databricks (Free Edition)

1. **Importar el notebook:** Workspace → tu carpeta → ⋮ → **Import** → File → selecciona `01_Lakehouse_Medallon_ESP32.py`.
2. Conéctalo a **Serverless** y ejecuta las celdas **00 → 6d** en orden.
   - La celda 00 imprime el **catálogo activo** (`proyecto_nota3` o `workspace` si tu cuenta no deja crear catálogos). Anótalo.
3. **ID del SQL Warehouse:** menú **SQL Warehouses** → *Serverless Starter Warehouse* → pestaña **Connection details** → en *HTTP path* (`/sql/1.0/warehouses/abc123...`) el ID es la última parte (`abc123...`).
4. **Token personal:** tu avatar (arriba a la derecha) → **Settings** → **Developer** → **Access tokens** → **Generate new token** (ponle 30 días de vida). Cópialo de una vez, no se vuelve a mostrar.
   - Si aparece el aviso *Verify identity*, complétalo primero.
   - ⚠️ No subas el token a GitHub ni lo pongas en el informe. Si se filtra, revócalo en la misma pantalla.

## 2. Arduino IDE

1. **Placas:** Archivo → Preferencias → *URLs adicionales*:
   `https://espressif.github.io/arduino-esp32/package_esp32_index.json` → Herramientas → Placa → Gestor de placas → instala **esp32 (Espressif)**.
2. **Librería:** Gestor de librerías → **ArduinoJson** (Benoit Blanchon, versión 7).
3. En **cada** sketch edita el bloque `CONFIGURACIÓN`: Wi-Fi, `DATABRICKS_HOST` (ya viene con tu workspace), `DATABRICKS_TOKEN`, `WAREHOUSE_ID` y `CATALOG`.
4. Placa: **ESP32 Dev Module**. Sube el sketch y abre el Monitor Serie a **115200**.

### Conexión de los 3 LEDs (igual en los dos ESP32)
Necesitas por cada placa: 3 LEDs (verde, amarillo, rojo), 3 resistencias de **220–330 Ω** y cables dupont.

```
GPIO 25 ──[ 220Ω ]──►|── GND   LED VERDE
GPIO 26 ──[ 220Ω ]──►|── GND   LED AMARILLO
GPIO 27 ──[ 220Ω ]──►|── GND   LED ROJO
          (pata larga = ánodo hacia la resistencia; pata corta = cátodo a GND)
```

| LED | ESP32 #1 (productor) | ESP32 #2 (dashboard) |
|---|---|---|
| Verde | Fijo: Wi-Fi OK y enviando · Parpadea: en pausa · Apagado: sin Wi-Fi | Fijo: datos de Gold al día · 3 destellos: llegaron eventos nuevos del ESP32 #1 |
| Amarillo | Encendido mientras transmite un lote | Encendido mientras consulta Databricks |
| Rojo | El último lote falló | La última consulta falló o no hay Wi-Fi |

Ambas placas: el **amarillo parpadea** mientras se conectan al Wi-Fi y, al encender, hacen un **autodiagnóstico** (verde → amarillo → rojo).
Se usan los GPIO 25, 26 y 27 porque no interfieren con el arranque de la placa (evita los GPIO 0, 2, 12 y 15).

### ESP32 #1 – Productor
- Envía un lote de 20 eventos cada 15 s (60 % compras) con `source='ESP32'` y su `device_id`.
- **Botón BOOT** = pausa/reanuda (el LED verde pasa a parpadear).
- Opcional (más electrónica): potenciómetro en **GPIO 34** (extremos a 3V3 y GND) y `USAR_POTENCIOMETRO 1` → el potenciómetro ajusta el intervalo entre 5 y 60 s en vivo.

### ESP32 #2 – Dashboard
- Consulta Databricks cada 30 s. El Monitor Serie muestra la IP; abre `http://<IP>` o `http://dashboard-esp32.local` desde el celular o el PC **en la misma red Wi-Fi**.
- Muestra: 4 KPIs, barras por región y categoría, líneas mensuales por región, filtros por región/categoría y la tabla con los últimos eventos del ESP32 #1.
- Las gráficas no necesitan internet en el navegador (están dibujadas con canvas).

## 3. Guion sugerido para la sustentación
1. Notebook celdas 0 → 5: generación, **evaluación perezosa** (celdas 1–4 no ejecutan nada, `explain` en la 4) y la acción real en la 5.
2. Celda 0b: **schema enforcement** (falla) vs **schema evolution** (`mergeSchema`, nace `device_id`).
3. 6a–6d: Gold con **Liquid Clustering** + `OPTIMIZE`.
4. **Encender ESP32 #1** → mostrar el LED y el Monitor Serie → celda 7 (`DESCRIBE HISTORY`: una versión por lote) y 7b.
5. Celda 9 (**MERGE**) → ejecutarla dos veces (la segunda inserta 0) → 9b refresca Gold.
6. **Página del ESP32 #2**: ver subir el contador de eventos y los totales tras el refresco. Usar los filtros.
7. Celdas 8, 10/10b, 11/11b y 12: time travel, small files + OPTIMIZE, RESTORE y VACUUM.
8. Dashboard formal en Databricks SQL / Power BI sobre `gold.*` (rúbrica: 30 pts).

## 4. Recomendaciones
- **Wi-Fi:** lleva el hotspot del celular como respaldo; ambos ESP32 y el PC deben estar en esa red. Los ESP32 solo funcionan con redes de **2,4 GHz** (no 5 GHz) y no con portales cautivos (como el Wi-Fi de la universidad con página de login).
- **Cuota de Free Edition:** cada envío mantiene encendido el SQL Warehouse. Pausa el ESP32 #1 con BOOT cuando no estés demostrando.
- **Primera consulta lenta:** si el warehouse estaba apagado, puede tardar unos segundos en despertar; el ESP32 #1 lo tolera (estado `PENDING`).
- **Refresco automático de Gold (opcional):** crea un Job en *Jobs & Pipelines* que ejecute las celdas 9 y 9b cada 10–15 minutos.
