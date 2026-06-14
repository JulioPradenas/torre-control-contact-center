# Guía — Dashboard de Torre de Control en Looker Studio (Módulo 4)

Guía paso a paso para armar el dashboard ejecutivo consumiendo las vistas de
BigQuery (Módulo 3). Looker Studio es 100% web y manual; esta guía cubre cada
clic, desde la conexión hasta las capturas finales.

**Fuente de datos:** la vista granular `torre_control.v_contactos_dashboard`
(fila por fila, con todas las dimensiones para filtrar y flags numéricos que
simplifican los KPIs).

---

## 0. Prerrequisitos

- Entrar a [lookerstudio.google.com](https://lookerstudio.google.com) con la
  **misma cuenta dueña de los datos**: `prad3nas@gmail.com`. Con otra cuenta no
  verás el proyecto `torre-control-cc`.
- Tener datos sembrados en BigQuery (`scripts/sembrar_datos_bigquery.py`) y las
  vistas creadas (`03_modelo/vistas_kpis.sql`).

> **Nota sobre el Sandbox:** al conectar BigQuery, Looker puede pedir elegir un
> "proyecto de facturación". Elige `torre-control-cc`; las consultas son `SELECT`
> y caen dentro del free tier (1 TB/mes), sin requerir tarjeta.

---

## 1. Conectar Looker Studio a BigQuery

1. En Looker Studio: **Create → Report** (o **Blank Report**).
2. Se abre el panel **Add data**. Busca y elige el conector **BigQuery**
   (de Google). Autoriza el acceso si lo pide (**Authorize**).
3. Navega: **My Projects → `torre-control-cc` → `torre_control` →
   `v_contactos_dashboard`**.
4. Clic en **Add** (abajo a la derecha). Confirma **Add to report** si aparece.

Ya tienes el reporte conectado a la vista granular.

---

## 2. Definir los KPIs como campos calculados

Estos campos se crean **una vez** a nivel de la fuente de datos y se reutilizan
en todos los tiles. En el panel **Data** (derecha) → en la fuente
`v_contactos_dashboard` → **Add a field** (o **Resource → Manage added data
sources → Edit → Add a field**).

Crea estos 6 campos (nombre → fórmula). Todos son **métricas** (agregadas):

| Nombre del campo | Fórmula en Looker |
|---|---|
| `SLA %` | `AVG(sla_flag) * 100` |
| `Abandono %` | `AVG(abandono_flag) * 100` |
| `FCR %` | `AVG(fcr_flag) * 100` |
| `AHT (s)` | `AVG(tiempo_atencion_seg)` |
| `CSAT prom` | `AVG(csat)` |
| `CPO` | `SUM(con_orden_flag) / COUNT_DISTINCT(orden_asociada)` |

> **Ojo con los nombres que ya existen.** En el panel **Data** ya aparecen los
> campos crudos de la tabla (icono `123` = numérico): `csat`,
> `tiempo_atencion_seg`, los `*_flag`, etc. Esos son valores **por contacto**, no
> KPIs. Por eso los campos calculados llevan nombres distintos (`CSAT prom`, no
> `CSAT`): Looker no permite repetir un nombre existente. Si arrastras un campo
> crudo numérico a un scorecard, Looker lo **suma** por defecto — para esos casos
> cambia la agregación a **Average**.

> **Por qué funcionan los `NULL`:** `fcr_flag` y `csat` son `NULL` en los
> abandonados / no-respondió. `AVG` ignora `NULL`, así que el FCR y el CSAT se
> calculan solo sobre quienes corresponde. Esto ya viene resuelto desde la vista.

Para cada uno: **Add a field** → pon el nombre → pega la fórmula → **Save**.

---

## 3. Estructura del dashboard

El layout objetivo (de arriba hacia abajo):

```
┌─────────────────────────────────────────────────────────────┐
│  TÍTULO: Torre de Control — Contact Center        [filtros]  │
├─────────┬─────────┬─────────┬─────────┬─────────┬───────────┤
│  SLA %  │  AHT    │ Aband % │  FCR %  │  CSAT   │    CPO     │  ← scorecards
├─────────┴─────────┴─────────┴─────────┴─────────┴───────────┤
│   SLA % por hora (línea)      │  Volumen por hora (barras)  │  ← tendencias
├───────────────────────────────┴─────────────────────────────┤
│   KPIs por canal (tabla)      │   KPIs por cola (tabla)     │  ← segmentación
├───────────────────────────────┴─────────────────────────────┤
│  Top 10 agentes por FCR       │  Heatmap hora × canal       │  ← operación
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Fila 1 — Scorecards (KPIs hero)

Para cada KPI: **Insert → Scorecard**, dibújalo arriba, y en el panel derecho
setea la **Metric** al campo correspondiente. Crea 6 scorecards:

| Scorecard | Metric | Formato sugerido |
|---|---|---|
| SLA | `SLA %` | número, 1 decimal, sufijo `%` |
| AHT | `AHT (s)` | número, 0 decimales, sufijo `s` |
| Abandono | `Abandono %` | número, 1 decimal, sufijo `%` |
| FCR | `FCR %` | número, 1 decimal, sufijo `%` |
| CSAT | `CSAT prom` | número, 2 decimales |
| CPO | `CPO` | número, 2 decimales |

Tip: selecciona un scorecard → pestaña **Style** para color y tamaño de fuente.
Copia/pega (Ctrl+C/Ctrl+V) para mantener tamaños consistentes.

---

## 5. Fila 2 — Tendencias horarias

**5.1 — SLA por hora (gráfico de línea)**
- **Insert → Line chart** (gráfico de líneas, **NO** Time series).
  *Time series solo acepta dimensiones de tipo fecha; `hora_del_dia` es un
  número (0–23), una categoría, no un instante.*
- **Dimension:** `hora_del_dia`.
- **Metric:** `SLA %`.
- En **Sort:** `hora_del_dia` ascendente (para que el eje X vaya 0→23).

**5.2 — Volumen por hora (barras)**
- **Insert → Column chart**.
- **Dimension:** `hora_del_dia`.
- **Metric:** `Record Count` (Looker la trae sola) o crea `COUNT(contact_id)`.
- **Sort:** `hora_del_dia` ascendente.

Estos dos juntos cuentan la historia clave: el volumen sube en los peaks y el
SLA se cae justo ahí.

---

## 6. Fila 3 — Segmentación (tablas)

**6.1 — KPIs por canal**
- **Insert → Table**.
- **Dimension:** `canal`.
- **Metrics:** `SLA %`, `AHT (s)`, `Abandono %`, `FCR %`, `CSAT prom`.
- **Lectura rápida:** selecciona la tabla → pestaña **Style** (arriba del panel
  derecho, junto a Setup) → baja a la lista de métricas; cada columna tiene un
  selector **Number / Bar / Heatmap**. Pon **Heatmap** en `SLA %` y `Abandono %`
  para que el canal problemático salte en rojo.

**6.2 — KPIs por cola**
- Igual que arriba, pero **Dimension:** `cola`.

---

## 7. Fila 4 — Operación

**7.1 — Top 10 agentes por FCR**
- **Insert → Table**.
- **Dimension:** `agente_id`.
- **Metrics:** `FCR %`, `AHT (s)`, `CSAT prom`, y `Record Count` (n° de contactos).
- **Sort:** `FCR %` descendente.
- **Filter** (panel derecho → Add filter): incluir solo atendidos. La forma más
  robusta: **Include** → `abandono_flag` → **Equal to** → `0` (los abandonados no
  tienen agente). Alternativa: **Exclude** → `agente_id` → **Is Null**.
- En **Style → Rows per page:** 10.

**7.2 — Heatmap hora × canal**
- **Insert → Pivot table**.
- **Row dimension:** `hora_del_dia`.
- **Column dimension:** `canal`.
- **Metric:** `SLA %` (o `Record Count` para ver volumen).
- **Style:** activa **Heatmap** en la métrica (rojo = bajo SLA, verde = alto).

---

## 8. Filtros globales

Estos controlan **todos** los tiles a la vez. Insértalos en la franja superior.

1. **Rango de fecha:** **Insert → Date range control**. Por defecto tomará
   `timestamp_evento` / `fecha`. Setéalo a "últimos 7 días" o "este mes".
2. **Canal:** **Insert → Drop-down list** → **Control field:** `canal`.
3. **Región:** **Insert → Drop-down list** → **Control field:** `region`.

Como la fuente es granular (`v_contactos_dashboard`), estos filtros **recortan
correctamente cada KPI y cada tabla** — esa fue la razón de usar la tabla cruda
y no las vistas ya agregadas.

> **Cross-filtering (filtrado cruzado).** Al hacer clic en un elemento de un
> gráfico (una barra, una porción), Looker filtra TODA la página por ese valor.
> Es una gracia de drill-down, no un error. Para volver al estado normal: clic de
> nuevo en el elemento, o en una zona vacía, o en el ícono de reset (⟲) que
> aparece arriba a la derecha del gráfico. Para desactivarlo en un gráfico:
> **Setup → Interactions → desmarcar Cross-filtering**.

---

## 9. Pulido y publicación

- **Tamaño del lienzo:** si los tiles de abajo se salen de la página, clic en
  zona vacía → **Theme and layout → Layout → Canvas size → Custom** y súbelo a
  ~**1200 × 1800** (o más). Da el alto que faltaba para las 4 filas. *(Alternativa:
  mover la Fila 4 a una segunda página con **Page → New page**.)*
- **Título:** **Insert → Text**, "Torre de Control — Contact Center".
- **Tema:** **Theme and layout → Theme**, elige uno sobrio (oscuro luce bien
  para una "torre de control").
- **Alineación:** selecciona varios tiles → clic derecho → **Align / Distribute**.
- **Compartir:** botón **Share** (arriba a la derecha). Para el portafolio,
  **Manage access → Anyone with the link → Viewer** y copia el link público.

---

## 10. Capturas para el README

1. Con datos sembrados y los filtros en un rango con volumen, toma **1 captura
   completa** del dashboard y **2-3 de detalle** (scorecards, heatmap).
2. Guárdalas en `04_dashboard/screenshots/`.
3. Avísame y las agrego a la sección **"Resultados"** del `README.md`, junto al
   link público del dashboard.

---

## Checklist de cierre del Módulo 4

- [ ] Reporte conectado a `v_contactos_dashboard`.
- [ ] 6 campos calculados creados (SLA, AHT, Abandono, FCR, CSAT, CPO).
- [ ] 6 scorecards.
- [ ] 2 gráficos de tendencia horaria.
- [ ] 2 tablas de segmentación (canal, cola).
- [ ] Tabla Top 10 agentes + heatmap hora×canal.
- [ ] 3 filtros globales (fecha, canal, región) funcionando.
- [ ] Capturas en `04_dashboard/screenshots/` + link público.
