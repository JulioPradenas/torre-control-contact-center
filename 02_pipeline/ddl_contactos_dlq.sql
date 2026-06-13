-- Dead Letter Queue del pipeline (Modulo 2).
-- Mensajes que no pudieron procesarse: JSON invalido o sin campos obligatorios.
-- Guardamos el payload crudo y el motivo, para inspeccion/reproceso posterior.
CREATE TABLE IF NOT EXISTS `torre-control-cc.torre_control.contactos_dlq` (
  raw_payload  STRING    OPTIONS(description="Mensaje original tal cual llego (texto)"),
  error        STRING    NOT NULL OPTIONS(description="Motivo del descarte"),
  ingested_at  TIMESTAMP NOT NULL OPTIONS(description="Instante de captura en la DLQ, UTC")
)
PARTITION BY DATE(ingested_at)
OPTIONS(description="Mensajes mal formados descartados por el pipeline de contactos");
