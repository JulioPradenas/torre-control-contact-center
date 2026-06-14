-- ============================================================================
-- 03_modelo/vistas_kpis.sql
-- ----------------------------------------------------------------------------
-- Vistas de KPIs operacionales sobre torre_control.contactos (Modulo 3).
-- Todas son CREATE OR REPLACE VIEW: solo SELECT, permitido en el Sandbox.
--
-- Convenciones de calculo (consistentes en todas las vistas):
--   SLA %       = 100 * AVG(CAST(dentro_sla AS INT64))
--   Abandono %  = 100 * AVG(CAST(abandonado AS INT64))
--   AHT (seg)   = AVG(tiempo_atencion_seg)  -> los NULL de abandonados se
--                 excluyen solos, no contaminan el promedio.
--   FCR %       = 100 * AVG(CAST(resuelto_primer_contacto AS INT64))
--   CSAT prom   = AVG(csat)  -> NULL (no respondio) excluido automaticamente.
-- ============================================================================


-- 1. KPIs globales: una sola fila con la foto de toda la operacion.
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_globales` AS
SELECT
    COUNT(*)                                                      AS total_contactos,
    ROUND(100 * AVG(CAST(dentro_sla AS INT64)), 1)               AS sla_pct,
    ROUND(100 * AVG(CAST(abandonado AS INT64)), 1)               AS abandono_pct,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg,
    ROUND(100 * AVG(CAST(resuelto_primer_contacto AS INT64)), 1) AS fcr_pct,
    ROUND(AVG(csat), 2)                                          AS csat_prom
FROM `torre-control-cc.torre_control.contactos`;


-- 2. KPIs por canal (voz, chat, app, correo).
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_por_canal` AS
SELECT
    canal,
    COUNT(*)                                                      AS total_contactos,
    ROUND(100 * AVG(CAST(dentro_sla AS INT64)), 1)               AS sla_pct,
    ROUND(100 * AVG(CAST(abandonado AS INT64)), 1)               AS abandono_pct,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg,
    ROUND(100 * AVG(CAST(resuelto_primer_contacto AS INT64)), 1) AS fcr_pct,
    ROUND(AVG(csat), 2)                                          AS csat_prom
FROM `torre-control-cc.torre_control.contactos`
GROUP BY canal;


-- 3. KPIs por cola / motivo de contacto. Revela donde esta el dolor operacional.
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_por_cola` AS
SELECT
    cola,
    COUNT(*)                                                      AS total_contactos,
    ROUND(100 * AVG(CAST(dentro_sla AS INT64)), 1)               AS sla_pct,
    ROUND(100 * AVG(CAST(abandonado AS INT64)), 1)               AS abandono_pct,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg,
    ROUND(100 * AVG(CAST(resuelto_primer_contacto AS INT64)), 1) AS fcr_pct,
    ROUND(AVG(csat), 2)                                          AS csat_prom
FROM `torre-control-cc.torre_control.contactos`
GROUP BY cola;


-- 4. KPIs por region. Para detectar diferencias geograficas.
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_por_region` AS
SELECT
    region,
    COUNT(*)                                                      AS total_contactos,
    ROUND(100 * AVG(CAST(dentro_sla AS INT64)), 1)               AS sla_pct,
    ROUND(100 * AVG(CAST(abandonado AS INT64)), 1)               AS abandono_pct,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg,
    ROUND(100 * AVG(CAST(resuelto_primer_contacto AS INT64)), 1) AS fcr_pct,
    ROUND(AVG(csat), 2)                                          AS csat_prom
FROM `torre-control-cc.torre_control.contactos`
GROUP BY region;


-- 5. KPIs por hora del dia (0-23). Muestra como SLA y abandono se degradan en peaks.
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_horarios` AS
SELECT
    EXTRACT(HOUR FROM timestamp_evento)                          AS hora,
    COUNT(*)                                                      AS total_contactos,
    ROUND(100 * AVG(CAST(dentro_sla AS INT64)), 1)               AS sla_pct,
    ROUND(100 * AVG(CAST(abandonado AS INT64)), 1)               AS abandono_pct,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg
FROM `torre-control-cc.torre_control.contactos`
GROUP BY hora
ORDER BY hora;


-- 6. KPIs por agente. Excluye abandonados (no tienen agente asignado).
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_kpis_por_agente` AS
SELECT
    agente_id,
    COUNT(*)                                                      AS contactos_atendidos,
    ROUND(AVG(tiempo_atencion_seg), 1)                           AS aht_seg,
    ROUND(100 * AVG(CAST(resuelto_primer_contacto AS INT64)), 1) AS fcr_pct,
    ROUND(AVG(csat), 2)                                          AS csat_prom
FROM `torre-control-cc.torre_control.contactos`
WHERE agente_id IS NOT NULL
GROUP BY agente_id;


-- 7. CPO (Contacts Per Order): contactos por orden de compra.
--    Solo sobre contactos que refieren a una orden (orden_asociada NO NULL).
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_cpo` AS
SELECT
    COUNT(*)                                                     AS contactos_con_orden,
    COUNT(DISTINCT orden_asociada)                              AS ordenes_distintas,
    ROUND(COUNT(*) / COUNT(DISTINCT orden_asociada), 2)         AS cpo
FROM `torre-control-cc.torre_control.contactos`
WHERE orden_asociada IS NOT NULL;


-- 8. Vista GRANULAR para Looker Studio (Modulo 4).
--    Fila por fila (no agregada), con todas las dimensiones para filtrar y
--    columnas de conveniencia que simplifican los campos calculados en Looker:
--      - fecha / hora_del_dia: ya extraidas del timestamp.
--      - *_flag: el bool casteado a INT64. Clave: fcr_flag conserva NULL para
--        los abandonados (CAST de NULL = NULL), asi AVG los excluye solos y el
--        FCR no se subestima. con_orden_flag marca los contactos con orden.
--    En Looker: SLA% = AVG(sla_flag)*100, AHT = AVG(tiempo_atencion_seg), etc.
CREATE OR REPLACE VIEW `torre-control-cc.torre_control.v_contactos_dashboard` AS
SELECT
    *,
    DATE(timestamp_evento)                          AS fecha,
    EXTRACT(HOUR FROM timestamp_evento)             AS hora_del_dia,
    CAST(dentro_sla AS INT64)                       AS sla_flag,
    CAST(abandonado AS INT64)                       AS abandono_flag,
    CAST(resuelto_primer_contacto AS INT64)         AS fcr_flag,
    IF(orden_asociada IS NOT NULL, 1, 0)            AS con_orden_flag
FROM `torre-control-cc.torre_control.contactos`;
