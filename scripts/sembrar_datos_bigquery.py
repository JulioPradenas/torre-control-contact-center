"""
scripts/sembrar_datos_bigquery.py
---------------------------------
Siembra un volumen realista de contactos directamente en BigQuery, para poder
validar las vistas de KPIs (Módulo 3) y armar el dashboard (Módulo 4) con datos
plausibles, sin esperar horas de streaming.

¿Por qué sembrar directo y no usar el pipeline?
El pipeline Pub/Sub → Beam → BigQuery ya quedó probado en el Módulo 2. Para la
capa analítica solo necesitamos VOLUMEN con una distribución horaria realista;
generarlo en vivo tomaría horas y concentraría todo en la hora actual. Esto es
poblar datos de análisis, una preocupación distinta de la ingesta en streaming.

Reutiliza generar_contacto() del generador (misma lógica de coherencia) y solo
sobreescribe el timestamp para repartirlo en el tiempo según FACTOR_HORARIO.

Uso:
    export GCP_PROJECT_ID=torre-control-cc
    export PYTHONPATH=.
    uv run python scripts/sembrar_datos_bigquery.py                  (2000 contactos, 3 días)
    uv run python scripts/sembrar_datos_bigquery.py --total 5000 --dias 7
    uv run python scripts/sembrar_datos_bigquery.py --append         (no recrea la tabla)
"""

# --- Librerías estándar -------------------------------------------------------
import argparse
import importlib.util
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

# --- Librería externa ---------------------------------------------------------
from google.cloud import bigquery

# --- Configuración del proyecto -----------------------------------------------
from config.settings import PROJECT_ID, DATASET_ID, TABLE_ID, FACTOR_HORARIO

# El generador vive en una carpeta "01_generador" (no es identificador Python
# válido para un import normal), así que lo cargamos por ruta de archivo.
_spec = importlib.util.spec_from_file_location(
    "generador", "01_generador/generador_contactos.py"
)
_generador = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_generador)
generar_contacto = _generador.generar_contacto

# Tamaño de lote por load job. 500 mantiene pocos jobs (2000/500 = 4) bajo el
# límite de 1.500 load jobs/tabla/día.
TAM_LOTE = 500


def timestamp_sintetico(dias_max: int) -> str:
    """Genera un timestamp UTC repartido en los últimos `dias_max` días.

    La hora del día se sortea ponderada por FACTOR_HORARIO, así la curva de
    volumen reproduce el patrón retail (valle de madrugada, peaks mañana/tarde).
    """
    dia_atras = random.randint(0, dias_max - 1)
    hora = random.choices(
        list(FACTOR_HORARIO.keys()), weights=list(FACTOR_HORARIO.values()), k=1
    )[0]
    momento = (datetime.now(timezone.utc) - timedelta(days=dia_atras)).replace(
        hour=hora, minute=random.randint(0, 59), second=random.randint(0, 59),
        microsecond=0,
    )
    return momento.isoformat()


def recrear_tabla(client: bigquery.Client, table_id: str) -> None:
    """Vacía la tabla recreándola desde la DDL versionada (el Sandbox no tiene DML)."""
    client.delete_table(table_id, not_found_ok=True)
    ddl = Path("02_pipeline/ddl_contactos.sql").read_text(encoding="utf-8")
    client.query(ddl).result()
    print(f"Tabla recreada vacía: {table_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Siembra contactos en BigQuery.")
    parser.add_argument("--total", type=int, default=2000, help="N° de contactos a sembrar.")
    parser.add_argument("--dias", type=int, default=3, help="Repartir en los últimos N días.")
    parser.add_argument("--append", action="store_true",
                        help="No recrear la tabla; agregar a lo existente.")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    if not args.append:
        recrear_tabla(client, table_id)

    # Genera todos los contactos con timestamp repartido, y los carga por lotes.
    contactos = []
    for _ in range(args.total):
        evento = generar_contacto()
        evento["timestamp_evento"] = timestamp_sintetico(args.dias)
        contactos.append(evento)

    cargados = 0
    for inicio in range(0, len(contactos), TAM_LOTE):
        lote = contactos[inicio:inicio + TAM_LOTE]
        client.load_table_from_json(lote, table_id, job_config=job_config).result()
        cargados += len(lote)
        print(f"  cargados {cargados}/{args.total}")

    print(f"Listo: {cargados} contactos sembrados en {table_id} (repartidos en {args.dias} días).")


if __name__ == "__main__":
    main()
