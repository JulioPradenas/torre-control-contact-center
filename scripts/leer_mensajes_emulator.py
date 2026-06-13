"""
scripts/leer_mensajes_emulator.py
---------------------------------
Lee mensajes de la subscripción en el emulador y los imprime en consola.

Útil para validar end-to-end:
    Generador → Topic → Subscripción → Este lector

El lector se queda escuchando hasta Ctrl+C.
"""

import json
from google.cloud import pubsub_v1

from config.settings import PROJECT_ID, TOPIC_ID

SUBSCRIPTION_ID = f"{TOPIC_ID}-sub"


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Se ejecuta cada vez que llega un mensaje desde la subscripción.

    - 'message.data' son los bytes que publicó el generador.
    - Decodificamos UTF-8 -> string, parseamos JSON -> dict, mostramos.
    - 'message.ack()' confirma al servidor que ya procesamos este mensaje;
      sin esto, Pub/Sub vuelve a entregarlo después del ack_deadline.
    """
    evento = json.loads(message.data.decode("utf-8"))
    estado = "ABANDONO" if evento["abandonado"] else f"AHT={evento['tiempo_atencion_seg']}s"
    print(f"← {evento['canal']:<6} | {evento['cola']:<22} | {estado}")
    message.ack()


def main() -> None:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Escuchando: {subscription_path}")
    print("Ctrl+C para detener.\n")

    # subscribe() arranca un hilo de fondo que llama a callback() por cada msg.
    # streaming_pull_future es el "asa" para mantenerlo vivo.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        # result() bloquea esta línea esperando que el futuro termine
        # (lo cual sólo pasa si hay error o si lo cancelamos con Ctrl+C).
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nLector detenido por el usuario.")


if __name__ == "__main__":
    main()