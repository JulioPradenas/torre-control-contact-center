"""
02_pipeline/pipeline_streaming.py
---------------------------------
Pipeline de Apache Beam que consume contactos del emulador de Pub/Sub en
streaming, los valida, y los aterriza en BigQuery (Sandbox).

¿Por qué Beam y no un consumidor simple?
Beam describe el flujo (leer → transformar → escribir) de forma declarativa y
el MISMO código corre local (DirectRunner) o en la nube (Dataflow). Aquí lo
corremos local contra el emulador, escribiendo a un BigQuery real.

Arquitectura híbrida (lo interesante del módulo):
    Pub/Sub LOCAL (emulador)  →  Beam en tu Mac  →  BigQuery CLOUD (Sandbox)

Restricción del Sandbox que define el diseño:
    El Sandbox NO permite streaming inserts ni FILE_LOADS (ambos exigen billing
    o un bucket de GCS). Sí permite LOAD JOBS cargados desde el cliente. Por eso
    NO usamos el conector WriteToBigQuery: un DoFn propio agrupa los eventos por
    ventana y los carga con load_table_from_json (load job gratuito, sin GCS).

Variables de entorno requeridas (cada terminal nueva):
    export PUBSUB_EMULATOR_HOST=localhost:8085
    export GCP_PROJECT_ID=torre-control-cc
    export PYTHONPATH=.

Uso:
    uv run python 02_pipeline/pipeline_streaming.py
    uv run python 02_pipeline/pipeline_streaming.py --ventana 30   (lotes más cortos)
"""

# --- Librerías estándar -------------------------------------------------------
import argparse
import json
import logging
from datetime import datetime, timezone

# --- Apache Beam --------------------------------------------------------------
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, StandardOptions, SetupOptions,
)
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows

# --- Cliente de BigQuery (para el DoFn de carga) ------------------------------
from google.cloud import bigquery

# --- Configuración del proyecto -----------------------------------------------
from config.settings import (
    PROJECT_ID, SUBSCRIPTION_ID,
    DATASET_ID, TABLE_ID, TABLE_DLQ_ID, VENTANA_LOTE_SEG,
)

# Los 8 campos que TODO contacto debe tener (los NOT NULL de la tabla). Si a un
# mensaje le falta uno, está corrupto: va a la DLQ en vez de perderse.
CAMPOS_OBLIGATORIOS = (
    "contact_id", "timestamp_evento", "canal", "cola", "region",
    "tiempo_espera_seg", "abandonado", "dentro_sla",
)

# Etiqueta de la salida secundaria del DoFn de validación. La salida principal
# son los eventos válidos; esta lleva los mensajes mal formados hacia la DLQ.
TAG_DLQ = "dlq"


def _registro_dlq(mensaje: bytes, error: str) -> dict:
    """Construye la fila para la tabla DLQ a partir de un mensaje fallido.

    Guardamos el payload crudo (decodificado de forma tolerante, para no fallar
    sobre los mismos bytes que ya estaban rotos) y el motivo del descarte.
    """
    return {
        "raw_payload": mensaje.decode("utf-8", errors="replace"),
        "error": error,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ------------------------------------------------------------------------------
# Etapa 2: parsear y validar
# ------------------------------------------------------------------------------
class ParsearYValidar(beam.DoFn):
    """bytes de Pub/Sub → dict validado, con salida secundaria a la DLQ.

    Un DoFn puede emitir a varias salidas "etiquetadas". Aquí:
      - salida principal: el evento válido (dict),
      - salida 'dlq': un registro con el payload crudo y el motivo del descarte.
    Así ningún mensaje malo se pierde en silencio: queda en la DLQ para inspección.
    """

    def process(self, mensaje: bytes):
        # bytes → str → dict. Si el JSON está roto, json.loads lanza excepción.
        try:
            evento = json.loads(mensaje.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            yield beam.pvalue.TaggedOutput(
                TAG_DLQ, _registro_dlq(mensaje, f"JSON inválido: {e}"))
            return

        # Validación: todos los campos obligatorios presentes y no nulos.
        faltantes = [c for c in CAMPOS_OBLIGATORIOS if evento.get(c) is None]
        if faltantes:
            yield beam.pvalue.TaggedOutput(
                TAG_DLQ, _registro_dlq(mensaje, f"Faltan campos obligatorios: {faltantes}"))
            return

        yield evento


# ------------------------------------------------------------------------------
# Etapa 4: cargar el lote a BigQuery
# ------------------------------------------------------------------------------
class CargarABigQuery(beam.DoFn):
    """Recibe (clave, [eventos]) de una ventana y los carga en UN load job.

    Por qué un solo job por ventana: cargar evento por evento gastaría la cuota
    de 1.500 load jobs/tabla/día en minutos. Agrupar es lo que hace viable el
    enfoque en el Sandbox.
    """

    def __init__(self, table_id: str):
        # table_id completo (proyecto.dataset.tabla). Parametrizado para reutilizar
        # el mismo DoFn en la tabla principal y en la DLQ.
        self._table_id = table_id

    def setup(self):
        # setup() corre UNA vez por worker, no por elemento. Crear el cliente de
        # BigQuery es caro (auth, conexiones): lo creamos aquí y lo reutilizamos.
        self._client = bigquery.Client(project=PROJECT_ID)
        # WRITE_APPEND: cada load job AGREGA filas, no reemplaza la tabla.
        self._job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

    def process(self, lote_con_clave):
        # GroupByKey entrega (clave, iterable_de_eventos). La clave es siempre la
        # misma (None): solo la usamos para forzar la agrupación, no nos importa.
        _, eventos = lote_con_clave
        filas = list(eventos)
        if not filas:
            return

        # load_table_from_json = load job gratuito, SIN pasar por GCS.
        # .result() bloquea hasta que termina y lanza excepción si falla.
        job = self._client.load_table_from_json(
            filas, self._table_id, job_config=self._job_config
        )
        job.result()
        logging.info("Cargadas %d filas a %s (job %s)",
                     len(filas), self._table_id, job.job_id)


# ------------------------------------------------------------------------------
# Construcción y ejecución del pipeline
# ------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline Beam: Pub/Sub (emulador) → BigQuery (Sandbox)."
    )
    parser.add_argument(
        "--ventana", type=int, default=VENTANA_LOTE_SEG,
        help="Tamaño de la ventana de micro-lotes en segundos (default desde settings).",
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    # --- Opciones del pipeline ------------------------------------------------
    options = PipelineOptions()
    # streaming=True: el pipeline no termina solo; corre hasta Ctrl+C.
    options.view_as(StandardOptions).streaming = True
    # DirectRunner: ejecuta en este proceso, en tu Mac. (DataflowRunner sería cloud.)
    options.view_as(StandardOptions).runner = "DirectRunner"
    # save_main_session: serializa los imports globales para que los DoFn los vean.
    options.view_as(SetupOptions).save_main_session = True

    # Ruta de la subscription en el emulador. Beam respeta PUBSUB_EMULATOR_HOST.
    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

    tabla_ok = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    tabla_dlq = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_DLQ_ID}"

    logging.info("Leyendo de: %s", subscription_path)
    logging.info("Escribiendo válidos a: %s", tabla_ok)
    logging.info("Escribiendo descartes (DLQ) a: %s", tabla_dlq)
    logging.info("Ventana de lote: %ds | Ctrl+C para detener.", args.ventana)

    # Helper: agrupa una PCollection en ventanas y carga cada lote a una tabla.
    # Cada paso lleva un sufijo distinto porque Beam exige nombres únicos por rama.
    def cargar_rama(pcoll, nombre, table_id):
        return (
            pcoll
            | f"Ventana_{nombre}" >> beam.WindowInto(FixedWindows(args.ventana))
            | f"Clave_{nombre}" >> beam.Map(lambda elem: (None, elem))
            | f"Agrupar_{nombre}" >> beam.GroupByKey()
            | f"Cargar_{nombre}" >> beam.ParDo(CargarABigQuery(table_id))
        )

    # --- El grafo (DAG) -------------------------------------------------------
    with beam.Pipeline(options=options) as p:
        # Leer + validar, con dos salidas: 'validos' (principal) y 'dlq'.
        resultado = (
            p
            | "LeerPubSub" >> ReadFromPubSub(subscription=subscription_path)
            | "ParsearValidar" >> beam.ParDo(ParsearYValidar()).with_outputs(
                TAG_DLQ, main="validos")
        )

        # Rama 1: eventos válidos → tabla de contactos.
        cargar_rama(resultado.validos, "validos", tabla_ok)
        # Rama 2: mensajes mal formados → tabla DLQ.
        cargar_rama(resultado[TAG_DLQ], "dlq", tabla_dlq)


if __name__ == "__main__":
    main()
