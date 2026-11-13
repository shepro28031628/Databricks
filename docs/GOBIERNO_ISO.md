# Gobierno, calidad y auditoría de datos

Capa transversal del proyecto, implementada en [03_Gobierno_Calidad_Auditoria.py](../03_Gobierno_Calidad_Auditoria.py). Alinea el Lakehouse con los conceptos de **ISO/IEC 38505-1** (gobierno de datos) e **ISO 8000** (calidad de datos), y agrega **control de acceso (RBAC/ABAC)** y **auditoría**.

> **Alcance.** Es una alineación académica con los conceptos de las normas, no una certificación ni una evaluación de conformidad. Los propietarios, las clasificaciones y los plazos de retención son una propuesta de ejemplo que el grupo debe ajustar.

El notebook **no modifica** Bronze, Silver ni Gold. Crea el esquema `gobierno` y dos vistas en `gold`, así que no afecta al notebook 02 ni a los ESP32. Es idempotente.

## Qué se crea

| Objeto | Tipo | Para qué sirve |
| --- | --- | --- |
| `gobierno.gov_catalogo_datos` | Tabla | Catálogo: propietario, custodio, clasificación, dato personal, finalidad y retención de cada tabla |
| `gobierno.gov_roles_acceso` | Tabla | Atributos de cada usuario (rol, región permitida, nivel de acceso a datos personales). Fuente de la política ABAC |
| `gobierno.dq_resultados` | Tabla | Histórico de cada ejecución de las reglas de calidad |
| `gobierno.vw_calidad_ultima` | Vista | Resultado de la última ejecución de calidad |
| `gobierno.audit_historial_delta` | Tabla | Copia del historial Delta (quién, cuándo, qué operación) |
| `gold.vw_ventas_gobernada` | Vista | `gold_sales_summary` con filtro de filas por región |
| `gold.vw_usuarios_gobernada` | Vista | `gold_user_metrics` con filtro por región y `user_id` enmascarado |

También agrega comentarios y las etiquetas `clasificacion` y `norma` a las tablas de Bronze, Silver y Gold.

## ISO/IEC 38505-1: gobierno de datos

La norma se apoya en el ciclo **Evaluar – Dirigir – Monitorear** y en la **rendición de cuentas**: cada conjunto de datos debe tener un responsable.

| Idea de la norma | Cómo se refleja en el proyecto |
| --- | --- |
| Rendición de cuentas | `gov_catalogo_datos`: cada tabla tiene propietario y custodio |
| Clasificación y valor de los datos | Columna `clasificacion` (Interna / Confidencial) y etiquetas en Unity Catalog |
| Uso conforme a la finalidad | Columna `finalidad` y `dato_personal` en el catálogo |
| Dirigir (políticas) | Reglas de acceso (`gov_roles_acceso`) y umbrales de calidad por capa |
| Monitorear | Resultados de calidad históricos y auditoría de cambios y accesos |

## ISO 8000: calidad de datos

15 reglas medibles, cada una asociada a una dimensión de calidad y a un umbral por capa. Bronze es el dato crudo y tolera defectos; Silver y Gold exigen 100 %.

| Dimensión | Ejemplo de regla | Capas |
| --- | --- | --- |
| Completitud | `event_id` no nulo; `region_nombre` informado | Bronze, Silver |
| Unicidad | `event_id` único (Bronze ≥ 98 %, Silver 100 %) | Bronze, Silver |
| Validez | `amount > 0`; categoría normalizada; región en el dominio válido | Bronze, Silver |
| Consistencia | `total_amount = amount × quantity`; totales de Gold = filas de Silver | Silver, Gold |
| Oportunidad | El ESP32 envió eventos en los últimos 60 min | Bronze |

Resultado de la primera ejecución real: 14 de 15 reglas en **OK**. La regla de **oportunidad** quedó en **ALERTA** porque no había ningún ESP32 enviando datos en ese momento; es el comportamiento esperado cuando la demo no está en marcha.

La comparación entre capas muestra el valor de la arquitectura Medallón: Bronze cumple la unicidad al 99,0 % y la validez de `amount` al 99,5 %, mientras que Silver llega al 100 %.

## Control de acceso

### ABAC (por atributos): aplicado

Las vistas gobernadas leen `current_user()` y consultan `gov_roles_acceso`:

| Atributo | Efecto |
| --- | --- |
| `region_permitida` | `*` ve todas las regiones; un valor concreto (por ejemplo `NORTE`) filtra las filas |
| `nivel_pii` | `FULL` ve el `user_id` real; `MASKED` ve un seudónimo (`USR-` + hash SHA-256 truncado) |
| `vigente` | Un usuario no vigente o ausente de la tabla no ve ninguna fila |

Perfiles de ejemplo (los dos últimos son **ficticios**, solo para demostrar la política):

| Usuario | Rol | Regiones | `user_id` |
| --- | --- | --- | --- |
| Propietario de la cuenta | ADMIN | Todas | Real |
| `analista.norte@ejemplo.com` | ANALISTA | NORTE | Enmascarado |
| `gerencia@ejemplo.com` | GERENCIA | Todas | Enmascarado |

La celda 6 del notebook simula lo que vería cada perfil con la misma lógica de la vista. Para que la política proteja de verdad, los consumidores deben leer las vistas y no las tablas base.

### RBAC (por roles): definido, no aplicado en esta cuenta

El notebook define `GRANT` de mínimo privilegio para cinco grupos:

| Grupo | Permisos |
| --- | --- |
| `grp_ingesta_iot` | Solo `SELECT` y `MODIFY` sobre `bronze.eventos_raw` (lo que necesita el ESP32 #1) |
| `grp_ingenieria_datos` | Todos los privilegios sobre `bronze`, `silver` y `gold` |
| `grp_analistas` | `SELECT` sobre las vistas gobernadas y las dimensiones, **sin acceso a las tablas base** |
| `grp_gerencia` | `SELECT` sobre las vistas gobernadas |
| `grp_auditoria` | `SELECT` sobre el esquema `gobierno` |

**Limitación:** los grupos se administran a nivel de cuenta y en esta cuenta solo existen `admins` y `users`. Por eso los `GRANT` se omiten y el notebook lo informa sin fallar. Con una cuenta que permita crear grupos, basta con crearlos y volver a ejecutar la celda 7.

## Auditoría

| Fuente | Qué registra | Estado |
| --- | --- | --- |
| Historial Delta (`audit_historial_delta`) | Cada versión de cada tabla: quién, cuándo y qué operación, incluidos los cambios en `gov_roles_acceso` | Activa. Se copia a una tabla propia porque el log de Delta se limpia con el tiempo |
| `system.access.audit` | Accesos y acciones de la plataforma por usuario | Disponible en esta cuenta (hubo eventos en el último día) |
| `SHOW GRANTS` | Permisos vigentes sobre esquemas y vistas | Disponible (celda 11) |

## Cómo ejecutarlo

1. Importa `03_Gobierno_Calidad_Auditoria.py` y pulsa **Run all** (después del notebook 01).
2. Vuelve a ejecutarlo cuando quieras: la calidad agrega una ejecución al histórico y la auditoría agrega solo las versiones nuevas.
3. Para ver la oportunidad en **OK**, ejecútalo con el ESP32 #1 enviando datos.

## Limitaciones y siguientes pasos

- Un único usuario real: la política ABAC se demuestra con simulación para los perfiles ficticios.
- Los ESP32 consultan con el token del propietario. En producción se usaría una **service principal** dedicada con permisos mínimos, y no un token personal.
- Los dashboards actuales leen las tablas base. Para aplicar ABAC de punta a punta, habría que apuntarlos a `vw_ventas_gobernada` y `vw_usuarios_gobernada`.
- Las reglas de calidad se pueden programar con el notebook 02 o con un Job propio.
