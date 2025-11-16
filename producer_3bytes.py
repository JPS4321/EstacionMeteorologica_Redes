from kafka import KafkaProducer
import random
import time
from sensor import generar_lectura

# ==================================================
# CONFIGURACIÓN DEL BROKER
# ==================================================
BROKER = "iot.redesuvg.cloud:9092"
TOPIC = "22102"  # usa tu número de carné

# ==================================================
# MAPA DE DIRECCIONES DE VIENTO
# ==================================================
wind_codes = {
    "N": 0, "NE": 1, "E": 2, "SE": 3,
    "S": 4, "SO": 5, "O": 6, "NO": 7
}

# ==================================================
# FUNCIÓN DE CODIFICACIÓN (JSON → 3 BYTES)
# ==================================================
def encode_data(data):
    temp = int(data["temperatura"] * 100)  # escala a centésimas
    hum = int(data["humedad"])
    wind = wind_codes[data["direccion_viento"]]

    # Empaquetar los 24 bits (14 + 7 + 3)
    packed = (temp << 10) | (hum << 3) | wind
    return packed.to_bytes(3, byteorder="big")

# ==================================================
# CREAR PRODUCER (sin serializer JSON)
# ==================================================
producer = KafkaProducer(bootstrap_servers=[BROKER])

print(f"\n Enviando datos codificados (3 bytes) a {BROKER} en el topic '{TOPIC}'")
print("Presiona Ctrl+C para detener.\n")

try:
    while True:
        # generar lectura simulada
        lectura = generar_lectura()
        encoded = encode_data(lectura)
        producer.send(TOPIC, value=encoded)
        producer.flush()

        print(f" Enviado: {lectura}  → bytes: {encoded}")
        time.sleep(random.randint(15, 30))

except KeyboardInterrupt:
    print("\n Envío detenido por el usuario.")
finally:
    producer.close()
