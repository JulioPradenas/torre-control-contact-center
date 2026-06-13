# Instrucciones para Claude Code — Proyecto Torre de Control

Este archivo es el plano operacional completo del proyecto. Léelo antes de
cualquier modificación. Para contexto narrativo del dominio, ver `README.md`.

---

## Tabla de contenidos

1. Resumen del proyecto
2. Estado actual por módulo
3. Stack y versiones
4. Estructura del proyecto
5. Convenciones de código (válidas para todos los módulos)
6. Módulo 1 — Generador y Pub/Sub (✅ completo)
7. Módulo 2 — Pipeline Beam → BigQuery (🚧 pendiente)
8. Módulo 3 — Modelo SQL de KPIs (🚧 pendiente)
9. Módulo 4 — Dashboard Looker Studio (🚧 pendiente)
10. Reglas para Claude Code
11. Estilo de interacción preferido por el usuario

---

## 1. Resumen del proyecto

Pipeline de analítica en streaming para una **torre de control de contact
center** retail. Diseñado sobre GCP (Pub/Sub → Apache Beam → BigQuery →
Looker Studio) y ejecutable localmente con emuladores y datos sintéticos.

Proyecto de portafolio para un cargo de **Business Analyst — Digital Analytics
& BI** orientado a operaciones de atención al cliente (referencia: Walmart
Chile / Omnicare).

Los KPIs centrales que el proyecto modela:

- **SLA** (Nivel de Servicio): % atendidos en ≤ umbral (estándar 80/20).
- **AHT** (Average Handle Time): tiempo medio de atención.
- **Abandono**: % de contactos que cuelgan antes de ser atendidos.
- **FCR** (First Contact Resolution): % resueltos al primer contacto.
- **CPO** (Contacts Per Order): contactos por orden de compra.
- **Ocupación y adherencia** de agentes.

---

## 2. Estado actual por módulo

| Módulo | Nombre | Estado |
|---|---|---|
| 1 | Generador + ingesta Pub/Sub | ✅ Completo, funcionando end-to-end |
| 2 | Pipeline Beam → BigQuery | ✅ Completo, funcionando end-to-end |
| 3 | Modelo SQL de KPIs | 🚧 Pendiente — plan documentado |
| 4 | Dashboard Looker Studio | 🚧 Pendiente — plan documentado |

**Regla:** los módulos 🚧 NO deben implementarse sin confirmación explícita
del usuario. El plan descrito abajo es referencia; la implementación se hace
paso a paso, con el usuario validando cada decisión.

---

## 3. Stack y versiones

| Capa | Tecnología | Versión / Detalle |
|---|---|---|
| Lenguaje | Python | `>=3.11` (en `pyproject.toml`) |
| Gestor de paquetes | uv | Usado para deps y ejecución |
| Mensajería | Google Cloud Pub/Sub | Emulador local en `localhost:8085` |
| Procesamiento | Apache Beam | `2.70.0` (DirectRunner local, streaming) |
| Almacenamiento | BigQuery Sandbox | Proyecto `torre-control-cc` (cuenta prad3nas), dataset `torre_control` |
| Visualización | Looker Studio | Módulo 4 |
| Diagramas | Mermaid (en README) | Renderizado automático por GitHub |
| Control de versiones | Git + GitHub | Convención: Conventional Commits |

**Repo público:** https://github.com/JulioPradenas/torre-control-contact-center

---

## 4. Estructura del proyecto

```
torre-control-contact-center/
├── pyproject.toml             # config de uv + deps
├── uv.lock                    # versiones exactas (versionado)
├── .python-version
├── .gitignore
├── README.md                  # documentación pública
├── CLAUDE.md                  # este archivo
├── config/
│   └── settings.py            # ÚNICO lugar de catálogos y parámetros
├── 01_generador/
│   └── generador_contactos.py # productor de eventos -> Pub/Sub
├── scripts/
│   ├── crear_topic_emulator.py
│   ├── crear_subscription_emulator.py
│   └── leer_mensajes_emulator.py
├── 02_pipeline/               # 🚧 PENDIENTE (Módulo 2)
│   └── pipeline_streaming.py  # Beam: Pub/Sub -> BigQuery/DuckDB
├── 03_modelo/                 # 🚧 PENDIENTE (Módulo 3)
│   ├── ddl_tablas.sql         # esquema de tablas
│   └── vistas_kpis.sql        # vistas de KPIs
└── 04_dashboard/              # 🚧 PENDIENTE (Módulo 4)
    └── guia_looker.md         # guía de armado del dashboard
```

---

## 5. Convenciones de código (válidas para todos los módulos)

### Estilo Python
- Python 3.11+, type hints en funciones públicas.
- Docstrings al estilo Google (`Args:`, `Returns:`) — ver `01_generador/generador_contactos.py` como referencia.
- Imports agrupados: stdlib → librerías externas → módulos del proyecto, con comentarios separadores.
- f-strings, no `.format()` ni `%`.

### Comentarios
- Explicar **el porqué** de una decisión, no el qué.
- Cualquier parámetro "mágico" (porcentaje, umbral, distribución) debe estar comentado con su justificación.

### Configuración
- Catálogos del negocio (canales, colas, regiones, pesos, AHT base) viven SOLO en `config/settings.py`.
- Identificadores cloud (PROJECT_ID) se leen de variables de entorno, nunca hardcodeados.
- Nombres de recursos GCP (topic, subscription) sí pueden ser constantes — no son secretos.

### Idempotencia
- Los scripts de `scripts/` deben ser idempotentes: si el recurso ya existe, lo informan sin error (catch `AlreadyExists`).

### Mensajes de commit
- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `test:`.
- Mensaje corto en imperativo: "feat: agregar pipeline Beam streaming".

### Gestión de dependencias
- Agregar deps con `uv add <paquete>`, nunca editar `pyproject.toml` a mano.
- `uv.lock` SÍ se versiona (reproducibilidad).

### Variables de entorno requeridas (cada terminal nueva)
```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PYTHONPATH=.
```

---

## 6. Módulo 1 — Generador y Pub/Sub (✅ completo)

### Objetivo
Simular la llegada de contactos a un contact center retail y publicarlos a
Pub/Sub como un proceso de Poisson con curva horaria realista.

### Archivos
- `config/settings.py` — catálogos y parámetros.
- `01_generador/generador_contactos.py` — productor.
- `scripts/crear_topic_emulator.py` — setup idempotente.
- `scripts/crear_subscription_emulator.py` — setup idempotente.
- `scripts/leer_mensajes_emulator.py` — consumidor de validación.

### Modelo de evento publicado

| Campo | Tipo | Notas |
|---|---|---|
| `contact_id` | str (UUID) | único por evento |
| `timestamp_evento` | str (ISO 8601) | **SIEMPRE UTC** |
| `canal` | str | `voz`, `chat`, `app`, `correo` |
| `cola` | str | de `COLAS` en settings |
| `region` | str | de `REGIONES` en settings |
| `tiempo_espera_seg` | float | distribución exponencial |
| `abandonado` | bool | prob. crece con la espera |
| `dentro_sla` | bool | `not abandonado AND espera <= UMBRAL_SLA_SEG` |
| `agente_id` | str / None | `AG-001`; `None` si abandonado |
| `tiempo_atencion_seg` | int / None | gaussiana centrada en AHT base |
| `resuelto_primer_contacto` | bool / None | FCR ~78% |
| `csat` | int (1-5) / None | ~60% responden |
| `orden_asociada` | str / None | `ORD-XXXXXX`, ~55% de contactos |

**Coherencia crítica:** un evento `abandonado=True` debe tener los 5 campos
de atención en `None` (no `0`, no strings vacías). Esto evita contaminar
promedios en BigQuery más adelante.

### Comandos del módulo

| Terminal | Comando |
|---|---|
| 1 — Emulador | `gcloud beta emulators pubsub start --host-port=localhost:8085` |
| 2 — Setup | `uv run python scripts/crear_topic_emulator.py && uv run python scripts/crear_subscription_emulator.py` |
| 3 — Generador | `uv run python 01_generador/generador_contactos.py --total 50 --tasa 30` |
| 4 — Lector (validación) | `uv run python scripts/leer_mensajes_emulator.py` |

### Argumentos del generador
- `--total N` (default `0` = infinito hasta Ctrl+C).
- `--tasa X` (default `30` = contactos/min en hora peak).

### Smoke test
```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
export PYTHONPATH=.
uv run python 01_generador/generador_contactos.py --total 10 --tasa 60
```
Resultado esperado: 10 líneas `[ N] canal | cola | estado`, sin errores.

---

## 7. Módulo 2 — Pipeline Beam → BigQuery (✅ completo)

### Cómo quedó implementado (decisiones reales)

> El plan original más abajo se conserva como referencia, pero la implementación
> final difiere en un punto clave por restricciones del Sandbox. Esto manda.

**Destino elegido:** BigQuery Sandbox. Proyecto `torre-control-cc` (cuenta
`prad3nas@gmail.com`), dataset `torre_control` en multi-región `US`.

**Restricción central descubierta (y verificada con datos reales):** el Sandbox
SIN billing NO permite:
- *streaming inserts* (exigen tarjeta),
- *FILE_LOADS* de Beam (necesita un bucket de GCS, que también exige tarjeta),
- *DML* (`DELETE`/`UPDATE`/`TRUNCATE`/`MERGE`).

Sí permite: `SELECT`, DDL (`CREATE`/`DROP`) y **load jobs** cargados desde el
cliente. Por eso **NO se usa el conector `WriteToBigQuery`**. En su lugar, un
`DoFn` propio (`CargarABigQuery`) agrupa los eventos por ventana de tiempo
(`FixedWindows`, default 60s) y los carga con `load_table_from_json` (load job
gratuito, sin GCS). La ventana respeta el límite de 1.500 load jobs/tabla/día.

**Para vaciar tablas** (no hay DML): `client.delete_table()` + recrear desde la
DDL versionada.

**Dead Letter Queue:** implementada. `ParsearYValidar` usa salida etiquetada;
los mensajes mal formados (JSON inválido o sin campos obligatorios) van a la
tabla `contactos_dlq` con su payload crudo y el motivo del descarte.

**Archivos:**
- `02_pipeline/pipeline_streaming.py` — el pipeline (DirectRunner, streaming).
- `02_pipeline/ddl_contactos.sql` — DDL de `contactos` (particionada por día, cluster canal/cola).
- `02_pipeline/ddl_contactos_dlq.sql` — DDL de `contactos_dlq`.
- Constantes (`SUBSCRIPTION_ID`, `DATASET_ID`, `TABLE_ID`, `TABLE_DLQ_ID`, `VENTANA_LOTE_SEG`) en `config/settings.py`.

**Comandos del smoke test (4 terminales).** Exportar en T2/T3/T4:
`PUBSUB_EMULATOR_HOST=localhost:8085`, `GCP_PROJECT_ID=torre-control-cc`, `PYTHONPATH=.`

| Terminal | Comando |
|---|---|
| 1 — Emulador | `gcloud beta emulators pubsub start --host-port=localhost:8085` |
| 2 — Setup | `uv run python scripts/crear_topic_emulator.py && uv run python scripts/crear_subscription_emulator.py` |
| 3 — Pipeline | `uv run python 02_pipeline/pipeline_streaming.py --ventana 20` |
| 4 — Generador | `uv run python 01_generador/generador_contactos.py --total 20 --tasa 60` |

Validado: 20/20 filas en `contactos`, 0 incoherencias (abandonados con atención NULL).

---

### Objetivo
Consumir eventos del topic en streaming, validar/transformar, y aterrizarlos
en una tabla de BigQuery (o DuckDB local) lista para análisis.

### Decisión pendiente: BigQuery Sandbox vs DuckDB
**Esta decisión debe ser tomada por el usuario al iniciar el módulo, no por
Claude Code unilateralmente.**

- **BigQuery Sandbox:** gratuito, sin tarjeta, cloud real, dialecto BigQuery puro. Mismo código que producción. Requiere cuenta Google.
- **DuckDB local:** 100% local, instantáneo, SQL compatible con buena parte del dialecto BigQuery. Cero dependencia cloud, pero requiere comentar la diferencia de dialecto al final del módulo.

### Stack a introducir
- `apache-beam[gcp]` (instalar con `uv add apache-beam[gcp]`).
- Si BigQuery: usa el conector nativo `WriteToBigQuery`.
- Si DuckDB: `uv add duckdb`, conector custom escribiendo en una tabla.

### Componentes Beam a construir
1. **`PipelineOptions`** — flag `streaming=True`, runner (DirectRunner local, DataflowRunner si cloud).
2. **`ReadFromPubSub`** — lectura de la subscription `contactos-eventos-sub`.
3. **`ParsearYValidar`** (DoFn) — bytes → JSON → dict, descarta mensajes con campos obligatorios faltantes.
4. **`WriteToBigQuery` / `WriteToDuckDB`** — destino final.

### Esquema de la tabla destino (`torre_control.contactos`)

Aplica el modelo de evento del Módulo 1 (sección 6). Tipos sugeridos:

```sql
contact_id                 STRING NOT NULL
timestamp_evento           TIMESTAMP NOT NULL
canal                      STRING NOT NULL
cola                       STRING NOT NULL
region                     STRING NOT NULL
tiempo_espera_seg          FLOAT64 NOT NULL
abandonado                 BOOL NOT NULL
dentro_sla                 BOOL NOT NULL
agente_id                  STRING                -- NULL si abandonado
tiempo_atencion_seg        INT64                 -- NULL si abandonado
resuelto_primer_contacto   BOOL                  -- NULL si abandonado
csat                       INT64                 -- puede ser NULL aunque atendido
orden_asociada             STRING                -- puede ser NULL aunque atendido
```

### Mejora recomendada al final del módulo: Dead Letter Queue
Mensajes mal formados (JSON inválido, campos faltantes) deben ir a una segunda
tabla `contactos_dlq` en vez de descartarse silenciosamente. Esto se nota como
buena práctica en entrevistas.

### Smoke test esperado (al cerrar el módulo)
1. Levantar emulador (T1).
2. Levantar pipeline (T2): `uv run python 02_pipeline/pipeline_streaming.py`.
3. Generar eventos (T3): `uv run python 01_generador/generador_contactos.py --total 20 --tasa 60`.
4. Consultar destino: `SELECT COUNT(*) FROM torre_control.contactos` debe devolver 20.

---

## 8. Módulo 3 — Modelo SQL de KPIs (🚧 pendiente — plan)

### Objetivo
Construir las vistas SQL que transforman la tabla cruda de contactos en los
KPIs operacionales que consumirá el dashboard.

### Artefactos
- `03_modelo/ddl_tablas.sql` — DDL de tablas (si no fue ya hecho en M2).
- `03_modelo/vistas_kpis.sql` — definición de las vistas KPI.

### Vistas a implementar (en orden sugerido)

1. **`v_kpis_globales`** — métrica única por todo el dataset.
   - SLA global, AHT global, % abandono, FCR global, CSAT promedio, total contactos.

2. **`v_kpis_por_canal`** — agrupado por canal.
   - Mismas métricas, segmentadas por canal.

3. **`v_kpis_por_cola`** — agrupado por motivo de contacto.
   - Permite ver dónde está el dolor operacional.

4. **`v_kpis_por_region`** — agrupado por región.
   - Para detectar diferencias geográficas.

5. **`v_kpis_horarios`** — agrupado por hora del día.
   - Muestra cómo SLA y abandono se degradan en peaks.

6. **`v_kpis_por_agente`** — agrupado por agente.
   - AHT promedio, FCR, CSAT, n° contactos. Excluye `agente_id IS NULL`.

7. **`v_cpo`** — Contacts Per Order.
   - Cuenta de contactos por `orden_asociada` (excluyendo NULL).

### Patrones de SQL a respetar

- **Cálculo de SLA:** `AVG(CAST(dentro_sla AS INT64))` o equivalente. En %: multiplicar por 100.
- **Cálculo de abandono:** `AVG(CAST(abandonado AS INT64))`.
- **AHT:** `AVG(tiempo_atencion_seg)` solo sobre `WHERE NOT abandonado`. Los NULL de abandonados se excluyen automáticamente.
- **FCR:** `AVG(CAST(resuelto_primer_contacto AS INT64))` sobre `WHERE NOT abandonado`.
- **CSAT promedio:** `AVG(csat)`, los NULL no contaminan.
- **CPO:** `COUNT(*) / COUNT(DISTINCT orden_asociada)` filtrando `orden_asociada IS NOT NULL`.

### Validación al cerrar el módulo
Ejecutar cada vista y revisar plausibilidad:
- SLA global entre 30% y 90% (dependiendo de tasa de eventos).
- AHT por canal coherente con `aht_base` en settings (±30%).
- FCR entre 70-85%.
- % abandono entre 5-25%.

---

## 9. Módulo 4 — Dashboard Looker Studio (🚧 pendiente — plan)

### Objetivo
Dashboard ejecutivo de torre de control consumiendo las vistas del Módulo 3.

### Artefactos
- `04_dashboard/guia_looker.md` — guía paso a paso de armado en Looker Studio.
- Capturas de pantalla del dashboard en `04_dashboard/screenshots/` para el README.

### Estructura sugerida del dashboard

**Encabezado (KPIs hero, scorecards):**
- SLA global %, AHT global, % abandono, FCR global, CSAT promedio, CPO.

**Fila 2 — Tendencias:**
- SLA por hora del día (gráfico de línea).
- Volumen de contactos por hora (barras).

**Fila 3 — Segmentaciones:**
- KPIs por canal (tabla).
- KPIs por cola (tabla).

**Fila 4 — Operación:**
- Top 10 agentes por FCR.
- Heatmap hora × canal.

**Filtros globales:**
- Rango de fecha.
- Canal.
- Región.

### Conexión a datos
- Si M2 → BigQuery: conector nativo de Looker Studio.
- Si M2 → DuckDB: exportar a CSV/Parquet y subir, o usar BigQuery Sandbox como puente.

### Validación al cerrar el módulo
Capturas del dashboard agregadas al README (sección "Resultados") con las
métricas reales de la simulación corriendo.

---

## 10. Reglas para Claude Code

1. **Antes de tocar un archivo**, lee `config/settings.py`. La mayoría de los parámetros vive ahí.
2. **Antes de agregar una dependencia**, usar `uv add <paquete>`. Nunca editar `pyproject.toml` a mano para deps.
3. **Antes de implementar un módulo 🚧**, proponer el plan en chat y esperar confirmación. El usuario decide cada paso, no Claude.
4. **No regenerar archivos completos** cuando se pide editar uno existente. Usar ediciones puntuales (`str_replace` o equivalente).
5. **No saltar pasos** en los módulos en construcción. Cada paso necesita una pausa donde el usuario valida antes de seguir.
6. **Para conexiones a GCP**, mantener el patrón actual: el código detecta `PUBSUB_EMULATOR_HOST` automáticamente. No agregar lógica condicional manual local-vs-cloud.
7. **Commits**: Conventional Commits, en imperativo, en español o inglés (consistente con el resto del repo).
8. **Nunca subir** `.venv/`, `.env`, credenciales JSON o archivos excluidos por `.gitignore`.
9. **Antes de proponer "mejoras"**, considerar si están en el alcance del módulo actual. No expandir scope sin pedir.
10. **Si una decisión técnica importante aparece** (BigQuery vs DuckDB, runner local vs cloud, etc.), proponer las opciones con pros/contras y dejar que el usuario decida.

---

## 11. Estilo de interacción preferido por el usuario

El usuario (Julio) prefiere un estilo específico que **debe respetarse**:

- **Paso a paso, un concepto a la vez.** Nada de bloques masivos de código sin explicación previa.
- **Explicar el porqué antes de mostrar el código.** Contexto → decisión → código → verificación.
- **Pausar después de cada paso** para que el usuario lo ejecute antes de avanzar al siguiente.
- **No dar por sentado conocimiento previo** sobre infraestructura, GCP, herramientas. Si algo no se ha mencionado antes, explicar qué es.
- **Honestidad técnica sobre limitaciones:** si algo requiere tarjeta de crédito o setup adicional, decirlo de frente, no escabullir.
- **Respetar las decisiones de stack ya tomadas:** uv, emulador local de Pub/Sub, proyecto reproducible sin tarjeta.
- **Ofrecer alternativas cuando hay una restricción real**, no asumir que el usuario "encontrará la forma".