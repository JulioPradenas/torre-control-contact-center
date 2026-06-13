-- Tabla destino del pipeline de streaming (Modulo 2).
-- Proyecto: torre-control-cc | Dataset: torre_control
--
-- Nulabilidad: los 8 campos base son NOT NULL (todo contacto los tiene).
-- Los 5 campos de atencion son nulables: un contacto ABANDONADO los lleva en
-- NULL (nunca fue atendido). Marcarlos NOT NULL rechazaria todo abandono.
--
-- Particionado por dia y clustering por canal/cola: abaratan las consultas del
-- dashboard (filtros por fecha y canal) y respetan el limite de 1 TB/mes del Sandbox.
CREATE TABLE IF NOT EXISTS `torre-control-cc.torre_control.contactos` (
  contact_id                STRING    NOT NULL OPTIONS(description="UUID unico del contacto"),
  timestamp_evento          TIMESTAMP NOT NULL OPTIONS(description="Instante del evento, SIEMPRE UTC"),
  canal                     STRING    NOT NULL OPTIONS(description="voz | chat | app | correo"),
  cola                      STRING    NOT NULL OPTIONS(description="Motivo de contacto"),
  region                    STRING    NOT NULL OPTIONS(description="Region de origen"),
  tiempo_espera_seg         FLOAT64   NOT NULL OPTIONS(description="Espera en cola (seg)"),
  abandonado                BOOL      NOT NULL OPTIONS(description="Colgo antes de ser atendido"),
  dentro_sla                BOOL      NOT NULL OPTIONS(description="Atendido dentro del umbral SLA"),
  agente_id                 STRING             OPTIONS(description="AG-NNN; NULL si abandonado"),
  tiempo_atencion_seg       INT64              OPTIONS(description="AHT (seg); NULL si abandonado"),
  resuelto_primer_contacto  BOOL               OPTIONS(description="FCR; NULL si abandonado"),
  csat                      INT64              OPTIONS(description="1-5; NULL si no responde o abandonado"),
  orden_asociada            STRING             OPTIONS(description="ORD-NNNNNN; NULL si no aplica")
)
PARTITION BY DATE(timestamp_evento)
CLUSTER BY canal, cola
OPTIONS(description="Contactos del contact center ingeridos via Beam desde Pub/Sub");
