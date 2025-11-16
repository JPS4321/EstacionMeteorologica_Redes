from kafka import KafkaProducer
import json
import random
import time
from sensor import generar_lectura  # importa tu simulador

# --- CONFIGURACIÓN DEL BROKER ---
BROKER = "iot.redesuvg.cloud:9092"   
TOPIC = "22102"              

# --- CREAR PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

print(f"Enviando datos a {BROKER} (topic '{TOPIC}') cada 15–30 segundos.\nPresiona Ctrl+C para detener.\n")

try:
    while True:
        # generar los datos del sensor
        data = generar_lectura()
        key = "sensor1"

        # enviar a Kafka
        producer.send(TOPIC, key=key, value=data)
        producer.flush()

        print(f" Enviado: {data}")

        # espera aleatoria entre 15 y 30 segundos
        time.sleep(random.randint(15, 30))

except KeyboardInterrupt:
    print("\ Envió detenido por el usuario.")
finally:
    producer.close()
