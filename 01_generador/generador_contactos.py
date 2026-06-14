"""
01_generador/generador_contactos.py
-----------------------------------
Simula la llegada de contactos a un contact center retail y los publica,
uno por uno, a un topic de Pub/Sub.

¿Por qué streaming y no un CSV?
El cargo es "torre de control en tiempo real". Mostrar ingesta evento-por-evento
vía Pub/Sub demuestra el patrón GCP-nativo (Pub/Sub → procesamiento → BigQuery),
no un batch tradicional.

Uso:
    uv run python 01_generador/generador_contactos.py            (corre indefinido)
    uv run python 01_generador/generador_contactos.py --total 200 (publica 200 y termina)
    uv run python 01_generador/generador_contactos.py --tasa 5    (5 contactos/min, demo lenta)
"""

# --- Librerías estándar (vienen con Python, no se instalan) -------------------
import argparse              # para leer argumentos de línea de comandos (--total, --tasa)
import json                  # para serializar eventos a JSON antes de enviarlos
import random                # para muestreo aleatorio (canal, cola, AHT, etc.)
import time                  # para pausar entre eventos (time.sleep)
import uuid                  # para generar IDs únicos de contacto
from datetime import datetime, timezone  # para el timestamp del evento (en UTC)

# --- Librería externa (la instalamos con uv add) ------------------------------
from google.cloud import pubsub_v1  # cliente oficial de Pub/Sub para Python

# --- Configuración del proyecto (nuestro propio módulo) -----------------------
# Importamos TODA la configuración desde settings: el generador no conoce reglas
# "mágicas", solo consume catálogos. Esto lo hace mantenible y data-driven.
from config.settings import (
    PROJECT_ID, TOPIC_ID,
    CANALES, COLAS, REGIONES, N_AGENTES, N_ORDENES_POOL,
    FACTOR_HORARIO, CONTACTOS_POR_MINUTO_PEAK, UMBRAL_SLA_SEG,
)

# ------------------------------------------------------------------------------
# Utilidad: muestreo ponderado a partir de un diccionario {opción: peso}
# ------------------------------------------------------------------------------
def elegir_ponderado(opciones_con_pesos: dict) -> str:
    """
    Elige una clave del diccionario respetando los pesos como probabilidades.

    Ejemplo:
        elegir_ponderado({"voz": 0.45, "chat": 0.30, "app": 0.15, "correo": 0.10})
        → devuelve "voz" el 45% de las veces, "chat" el 30%, etc.

    ¿Por qué encapsular esto?
    - Nuestros catálogos son dicts; random.choices espera dos listas.
    - Da nombre a la operación y evita repetir la descomposición en cada uso.
    - random.choices devuelve [valor]; aquí desempacamos a valor para limpieza.
    """
    claves = list(opciones_con_pesos.keys())
    pesos = list(opciones_con_pesos.values())
    return random.choices(claves, weights=pesos, k=1)[0]

# ------------------------------------------------------------------------------
# Núcleo: generación de un contacto realista
# ------------------------------------------------------------------------------
def generar_contacto() -> dict:
    """
    Construye un único evento de contacto con campos coherentes entre sí.

    La coherencia es lo que hace que los KPIs del dashboard salgan correctos:
    un contacto ABANDONADO no puede tener AHT ni CSAT (nunca fue atendido),
    y los campos opcionales (CSAT, orden) reflejan que no siempre se capturan.
    """

    # --- Bloque 1: campos base que TODOS los contactos tienen -----------------

    # Canal: lo elegimos primero porque determina el AHT base.
    canal = elegir_ponderado({c: v["peso"] for c, v in CANALES.items()})
    aht_base = CANALES[canal]["aht_base"]

    # Motivo del contacto y región: muestreo ponderado y aleatorio simple.
    cola = elegir_ponderado(COLAS)
    region = random.choice(REGIONES)

    # Tiempo de espera en cola (segundos).
    # Usamos distribución EXPONENCIAL: la mayoría espera poco, pocos esperan
    # mucho. random.expovariate(1/media) -> media de ~18 s es realista.
    tiempo_espera_seg = round(random.expovariate(1 / 18), 1)

    # Regla de abandono: a mayor espera, mayor probabilidad de colgar.
    # Probabilidad lineal acotada a 0.9 (siempre queda 10% que aguanta).
    prob_abandono = min(0.9, tiempo_espera_seg / 90)
    abandonado = random.random() < prob_abandono

    # Construimos el dict con todos los campos comunes.
    evento = {
        "contact_id": str(uuid.uuid4()),                        # ID universalmente único
        "timestamp_evento": datetime.now(timezone.utc)          # SIEMPRE en UTC
                              .isoformat(),                     #   (estándar cloud)
        "canal": canal,
        "cola": cola,
        "region": region,
        "tiempo_espera_seg": tiempo_espera_seg,
        "abandonado": abandonado,
        # Dentro de SLA solo si fue atendido a tiempo Y no abandonó.
        "dentro_sla": (not abandonado) and (tiempo_espera_seg <= UMBRAL_SLA_SEG),
    }

    # --- Bloque 2: rama "abandonado" ------------------------------------------
    # El contacto colgó antes de ser atendido. Los campos de atención van NULL
    # (en Python: None; en JSON: null; en BigQuery: NULL).
    # ESTO ES CRÍTICO: poner ceros aquí contaminaría los promedios de AHT
    # y CSAT en el dashboard. None significa "no aplica", no "valor cero".
    if abandonado:
        evento.update({
            "agente_id": None,
            "tiempo_atencion_seg": None,
            "resuelto_primer_contacto": None,
            "csat": None,
            "orden_asociada": None,
        })
        return evento

    # --- Bloque 3: rama "atendido" --------------------------------------------
    # AHT con variación gaussiana centrada en el base del canal (±30%).
    # max(30, ...) evita valores absurdos: ningún contacto dura menos de 30 s.
    aht = max(30, round(random.gauss(aht_base, aht_base * 0.3)))

    # FCR (First Contact Resolution): se resuelve al primer contacto ~78%.
    # Es una de las métricas estrella en Omnicare/Walmart, por eso explícita.
    fcr = random.random() < 0.78

    # CSAT (1-5): solo ~60% de los atendidos deja encuesta (realista).
    # La distribución depende de si se resolvió o no al primer contacto:
    # con FCR los puntajes se inclinan a 4-5; sin FCR, más planos y bajos.
    if random.random() < 0.60:
        if fcr:
            csat = random.choices([1, 2, 3, 4, 5], weights=[5,  8, 15, 32, 40], k=1)[0]
        else:
            csat = random.choices([1, 2, 3, 4, 5], weights=[18, 22, 25, 20, 15], k=1)[0]
    else:
        csat = None  # cliente no respondió la encuesta

    # Asociación a orden de compra: ~55% de los contactos refieren a una orden.
    # La orden sale de un POOL acotado (N_ORDENES_POOL) para que varios contactos
    # compartan la misma orden; sin eso, CPO siempre daría 1. Permite calcular
    # CPO (contactos por orden) de forma realista más adelante.
    orden = f"ORD-{random.randint(1, N_ORDENES_POOL):06d}" if random.random() < 0.55 else None

    # Agente asignado: id sintético dentro del rango de la dotación.
    # El formato AG-001 con padding facilita ordenar y filtrar en el dashboard.
    agente_id = f"AG-{random.randint(1, N_AGENTES):03d}"

    evento.update({
        "agente_id": agente_id,
        "tiempo_atencion_seg": aht,
        "resuelto_primer_contacto": fcr,
        "csat": csat,
        "orden_asociada": orden,
    })

    return evento

# ------------------------------------------------------------------------------
# Publicación a Pub/Sub
# ------------------------------------------------------------------------------
def publicar(publisher, topic_path: str, evento: dict) -> None:
    """
    Publica un evento al topic de Pub/Sub.

    Flujo:
        dict Python → JSON string → bytes UTF-8 → Pub/Sub

    El cliente 'publisher' se recibe como parámetro (inyección de dependencia):
    crearlo es caro, así que el orquestador lo crea una sola vez y lo reutiliza.

    Args:
        publisher:   instancia de pubsub_v1.PublisherClient ya inicializada
        topic_path:  ruta completa del topic, ej. "projects/X/topics/Y"
        evento:      diccionario con los campos del contacto (ver generar_contacto)
    """

    # --- Serialización: dict -> JSON -> bytes ---------------------------------
    # json.dumps convierte el dict a una cadena JSON.
    # .encode("utf-8") la convierte a bytes (lo que Pub/Sub realmente transporta).
    data = json.dumps(evento).encode("utf-8")

    # --- Publicación asíncrona ------------------------------------------------
    # publish() retorna un Future: no espera la confirmación del servidor.
    # Esto permite alto rendimiento (Pub/Sub agrupa envíos internamente).
    #
    # Adjuntamos 'canal' como ATRIBUTO del mensaje, no como parte del payload.
    # Esto permite que un consumidor filtre por canal sin abrir el JSON.
    future = publisher.publish(
        topic_path,
        data=data,
        canal=evento["canal"],   # atributo extra; viaja junto al mensaje
    )

    # --- Manejo de errores asíncrono ------------------------------------------
    # Como publish() no espera, los errores no aparecen al instante.
    # Adjuntamos un callback que se ejecuta cuando el envío termina:
    # - Si fue exitoso: no hacemos nada (None).
    # - Si falló: imprimimos el error para no perderlo silenciosamente.
    #
    # Nota: NO llamamos future.result() acá. Eso bloquearía hasta tener
    # confirmación y mataría el rendimiento del streaming.
    future.add_done_callback(
        lambda f: None if not f.exception() else print(f"  ⚠ Falló publicación: {f.exception()}")
    )
    
    # ------------------------------------------------------------------------------
# Cálculo del intervalo entre contactos (proceso de Poisson)
# ------------------------------------------------------------------------------
def segundos_hasta_proximo_contacto(tasa_peak_por_min: float) -> float:
    """
    Calcula cuántos segundos esperar antes del próximo contacto.

    Modela las llegadas como un PROCESO DE POISSON:
    - Si los eventos ocurren a tasa λ por segundo,
    - el tiempo entre eventos sigue distribución exponencial con media 1/λ.

    Por qué no usar un intervalo fijo:
    Un intervalo constante (un contacto cada 2.0 s exactos) produce un flujo
    artificial, robótico. En la realidad hay ráfagas y silencios. La
    distribución exponencial reproduce esa irregularidad natural.

    Args:
        tasa_peak_por_min: contactos por minuto en hora peak (factor = 1.0)

    Returns:
        Tiempo de espera en segundos hasta el próximo contacto.
    """
    # Tomamos la hora actual del sistema (0-23) y consultamos el factor.
    # .get(hora, 0.5) es un fallback por si la hora no estuviera en el dict
    # (no debería pasar, pero defensivo no cuesta nada).
    hora = datetime.now().hour
    factor = FACTOR_HORARIO.get(hora, 0.5)

    # Ajustamos la tasa peak por el factor horario.
    # max(0.1, ...) evita una tasa de 0 (que haría expovariate dividir por cero).
    contactos_por_min = max(0.1, tasa_peak_por_min * factor)
    contactos_por_seg = contactos_por_min / 60

    # expovariate(λ) sortea un valor de la distribución exponencial.
    # El resultado es el "tiempo hasta el próximo evento" en segundos.
    return random.expovariate(contactos_por_seg)

# ------------------------------------------------------------------------------
# Punto de entrada del programa
# ------------------------------------------------------------------------------
def main() -> None:
    """
    Orquesta el ciclo principal: parsea argumentos, crea el cliente Pub/Sub,
    y entra al loop generar-publicar-esperar hasta que el usuario corte (Ctrl+C)
    o se alcance el total de eventos solicitado.
    """

    # --- 1. Parseo de argumentos de línea de comandos -------------------------
    # argparse construye una interfaz CLI profesional con --help, validación
    # de tipos y mensajes de error automáticos.
    parser = argparse.ArgumentParser(
        description="Generador de contactos -> Pub/Sub (torre de control)"
    )
    parser.add_argument(
        "--total", type=int, default=0,
        help="N° de eventos a publicar (0 = infinito, hasta Ctrl+C).",
    )
    parser.add_argument(
        "--tasa", type=float, default=CONTACTOS_POR_MINUTO_PEAK,
        help="Contactos/min en hora peak. Bájalo para una demo más lenta.",
    )
    args = parser.parse_args()

    # --- 2. Cliente de Pub/Sub (una sola instancia, reutilizada) --------------
    # Crear un PublisherClient es caro (conexiones, hilos, buffers internos).
    # Lo creamos UNA vez acá y se reutiliza durante toda la corrida.
    publisher = pubsub_v1.PublisherClient()

    # topic_path construye la ruta completa con formato:
    #   projects/<PROJECT_ID>/topics/<TOPIC_ID>
    # Es lo que Pub/Sub espera; no se le pasan los IDs sueltos.
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # --- 3. Mensaje inicial para el operador ----------------------------------
    print(f"Publicando en: {topic_path}")
    print(f"Modo: {'infinito (Ctrl+C para detener)' if args.total == 0 else f'{args.total} eventos'}")
    print(f"Tasa peak: {args.tasa} contactos/min\n")

    # --- 4. Loop principal ----------------------------------------------------
    enviados = 0
    try:
        # Condición: si total = 0 corremos para siempre; si no, hasta llegar.
        while args.total == 0 or enviados < args.total:
            # a) Generar un contacto realista (parte 2).
            evento = generar_contacto()

            # b) Publicarlo a Pub/Sub (parte 3).
            publicar(publisher, topic_path, evento)
            enviados += 1

            # c) Log compacto en consola para ver el flujo en tiempo real.
            #    Mostramos canal, cola y un resumen del estado del contacto.
            if evento["abandonado"]:
                estado = "ABANDONO"
            else:
                estado = f"AHT={evento['tiempo_atencion_seg']}s"
            print(f"[{enviados:>5}] {evento['canal']:<6} | {evento['cola']:<22} | {estado}")

            # d) Esperar hasta el próximo contacto (ritmo Poisson + hora).
            time.sleep(segundos_hasta_proximo_contacto(args.tasa))

    except KeyboardInterrupt:
        # Captura limpia de Ctrl+C. Sin esto, salía un traceback feo.
        print("\nInterrumpido por el usuario.")
    finally:
        # 'finally' se ejecuta SIEMPRE, haya error o no.
        # stop() vacía cualquier mensaje pendiente en el buffer interno
        # antes de salir, evitando que se pierdan los últimos eventos.
        publisher.stop()
        print(f"Total publicado: {enviados} eventos.")


# ------------------------------------------------------------------------------
# Guard: solo se ejecuta si corremos este archivo directamente,
# no si alguien lo importa como módulo.
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main() 
    