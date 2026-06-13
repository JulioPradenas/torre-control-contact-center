"""
scripts/crear_topic_emulator.py
-------------------------------
Crea el topic en el emulador de Pub/Sub.

Ejecutar UNA vez tras levantar el emulador con:
    gcloud beta emulators pubsub start --host-port=localhost:8085

Y requiere la variable de entorno PUBSUB_EMULATOR_HOST=localhost:8085
definida en la misma terminal donde se corre este script.

Si el topic ya existe, lo informa y termina sin error (idempotente).
"""

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Reutilizamos los identificadores del proyecto, NO los hardcodeamos acá.
# Esto garantiza que el topic creado coincide exactamente con el que el
# generador intentará usar.
from config.settings import PROJECT_ID, TOPIC_ID


def main() -> None:
    # Cliente publisher (el mismo tipo que usa el generador).
    # Detecta automáticamente PUBSUB_EMULATOR_HOST si está definido.
    publisher = pubsub_v1.PublisherClient()

    # Construye la ruta completa "projects/X/topics/Y".
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    try:
        # create_topic crea el topic en el emulador.
        # Pasamos el argumento como dict porque la firma actual de la librería
        # exige formato request= {...}.
        publisher.create_topic(request={"name": topic_path})
        print(f"✓ Topic creado: {topic_path}")
    except AlreadyExists:
        # Idempotencia: si el topic ya existe, no es un error.
        # Permite reejecutar el script sin preocuparse.
        print(f"= Topic ya existía: {topic_path}")


if __name__ == "__main__":
    main()