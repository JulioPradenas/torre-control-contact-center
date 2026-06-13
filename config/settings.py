"""
config/settings.py
------------------
Configuración central del proyecto.

Acá viven TODOS los parámetros: identificadores de GCP, catálogos del negocio
(canales, colas, regiones) y parámetros de la simulación.

¿Por qué un archivo separado y no constantes regadas por el código?
Para poder ajustar el negocio (agregar un canal, cambiar la curva horaria)
sin tocar la lógica del generador. Una buena práctica que se nota.
"""

import os

# --- Identificadores de GCP ---------------------------------------------------
# El ID del proyecto lo leemos de una variable de entorno (GCP_PROJECT_ID).
# Esto evita dejarlo escrito en el código (mala práctica: contamina el repo
# público y dificulta usar el proyecto en otros ambientes).
#
# Si la variable no está definida, usamos un placeholder feo a propósito.
# Para el emulador local cualquier valor sirve, pero mantenemos la disciplina.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tu-proyecto-gcp-aqui")

# Nombre del topic donde se publicarán los eventos de contactos.
# No es un secreto: es parte de la convención del proyecto, así que va fijo.
TOPIC_ID = "contactos-eventos"

# Subscription desde donde el pipeline (Módulo 2) consume los eventos.
# Convención: nombre del topic + sufijo "-sub". Un solo lugar de la verdad,
# así el script de setup y el pipeline leen el mismo nombre.
SUBSCRIPTION_ID = f"{TOPIC_ID}-sub"

# --- Destino BigQuery (Módulo 2) ----------------------------------------------
# Dataset y tabla donde el pipeline aterriza los contactos. Nombres de recurso
# (no secretos), por eso van fijos. El proyecto sí sale de GCP_PROJECT_ID arriba.
DATASET_ID = "torre_control"
TABLE_ID = "contactos"

# Tabla de Dead Letter Queue: mensajes corruptos (JSON inválido o sin campos
# obligatorios) que el pipeline no pudo procesar. En vez de descartarlos en
# silencio, los aterrizamos aquí para poder inspeccionarlos y reprocesarlos.
TABLE_DLQ_ID = "contactos_dlq"

# --- Catálogos del negocio ----------------------------------------------------
# Cada canal trae su PESO (probabilidad de ocurrencia) y AHT base en segundos
# (Average Handle Time = tiempo medio de atención de un contacto atendido).
#
# Por qué estos valores: reflejan patrones típicos de contact centers retail.
# - Voz domina con 45%: sigue siendo el canal preferido para reclamos.
# - Correo pesa solo 10% pero su AHT es el más alto (600s): es asíncrono.
# - Chat tiene AHT menor que voz porque el agente atiende varios en paralelo.
CANALES = {
    "voz":    {"peso": 0.45, "aht_base": 320},
    "chat":   {"peso": 0.30, "aht_base": 210},
    "app":    {"peso": 0.15, "aht_base": 180},
    "correo": {"peso": 0.10, "aht_base": 600},
}

# Colas / motivos de contacto. El valor es la probabilidad de que un contacto
# ENTRANTE caiga en esa cola. Suma 1.0.
#
# Por qué esta distribución: en retail, las consultas de despacho y postventa
# concentran el grueso de la demanda. Información general es residual.
COLAS = {
    "Despacho y entregas": 0.35,
    "Postventa":           0.25,
    "Reclamos":            0.20,
    "Pagos y facturación": 0.12,
    "Información general":  0.08,
}

# Regiones desde donde puede originarse el contacto. Para Walmart Chile
# elegimos las que concentran la operación; permitirá segmentar el dashboard.
REGIONES = [
    "Metropolitana",
    "Valparaíso",
    "Biobío",
    "Maule",
    "La Araucanía",
    "Antofagasta",
    "Coquimbo",
]

# Tamaño de la dotación de agentes en la simulación.
# Número intermedio: chico como para que se note la ocupación en peaks,
# grande como para tener variedad de agentes en el dashboard.
N_AGENTES = 40

# --- Parámetros de la simulación ----------------------------------------------
# FACTOR_HORARIO: multiplicador de demanda por hora del día (0-23).
#
# Modela el patrón típico de retail: valle de madrugada, peak de mañana y
# segundo peak fuerte por la tarde. El factor 1.0 es la hora de máxima
# demanda; el generador multiplicará la tasa peak por este factor según la
# hora actual del sistema.
FACTOR_HORARIO = {
    0:  0.10, 1:  0.05, 2:  0.05, 3:  0.05, 4:  0.05, 5:  0.10,
    6:  0.30, 7:  0.60, 8:  0.90, 9:  1.00, 10: 1.00, 11: 0.90,
    12: 0.80, 13: 0.70, 14: 0.80, 15: 0.90, 16: 1.00, 17: 0.95,
    18: 0.80, 19: 0.60, 20: 0.40, 21: 0.30, 22: 0.20, 23: 0.15,
}

# Tasa base: contactos por minuto en hora peak (cuando el factor horario = 1.0).
# Valor calibrado para una simulación "interesante pero manejable":
# - Suficiente para ver tendencias en el dashboard.
# - Bajo enough como para no saturar el emulador local.
# Se puede sobreescribir desde línea de comandos al lanzar el generador.
CONTACTOS_POR_MINUTO_PEAK = 30

# Umbral de Nivel de Servicio (SLA) en segundos.
# Un contacto se considera "dentro de SLA" si fue atendido en <= este tiempo
# de espera. 20 segundos es el estándar clásico del 80/20 (atender al 80%
# de los contactos en 20 segundos o menos).
UMBRAL_SLA_SEG = 20

# --- Parámetros del pipeline (Módulo 2) ---------------------------------------
# Tamaño de la ventana de micro-lotes, en segundos. El pipeline junta todos los
# eventos que llegan en esta ventana y los escribe a BigQuery en UN solo load job.
#
# Por qué 60s: BigQuery limita los load jobs a 1.500 por tabla al día. Una
# ventana de 60s da como máximo absoluto ~1.440 jobs/día, justo bajo el límite.
# Bajarla acelera la aparición de datos pero arriesga reventar la cuota si el
# pipeline corre de forma continua. 60s es el equilibrio latencia/cuota.
VENTANA_LOTE_SEG = 60