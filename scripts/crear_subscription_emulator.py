"""
scripts/crear_subscription_emulator.py
--------------------------------------
Crea la subscripción al topic en el emulador.

Idempotente: si la subscripción ya existe, lo informa sin error.
"""

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

from config.settings import PROJECT_ID, TOPIC_ID

# Nombre de la subscripción. Convención: nombre del topic + "-sub".
SUBSCRIPTION_ID = f"{TOPIC_ID}-sub"


def main() -> None:
    # SubscriberClient: cliente del lado consumidor. Distinto de PublisherClient.
    subscriber = pubsub_v1.SubscriberClient()

    # Construimos las dos rutas: del topic (origen) y de la subscripción (nueva).
    topic_path = subscriber.topic_path(PROJECT_ID, TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"✓ Subscripción creada: {subscription_path}")
    except AlreadyExists:
        print(f"= Subscripción ya existía: {subscription_path}")


if __name__ == "__main__":
    main()