import random
import json
import time

# --- Funciones individuales para cada sensor ---

def sensor_temperatura():
    """
    Genera una temperatura aleatoria con distribución normal (gaussiana),
    centrada alrededor de 55°C, y con desviación de 20°C.
    Se limita al rango [0, 110].
    """
    valor = random.gauss(55, 20)   # media=55, desviación=20
    valor = max(0, min(110, valor))
    return round(valor, 2)


def sensor_humedad():
    """
    Genera una humedad relativa (entera) con distribución normal.
    Media 50%, desviación 20%, limitada a [0, 100].
    """
    valor = int(random.gauss(50, 20))
    valor = max(0, min(100, valor))
    return valor


def sensor_viento():
    """
    Devuelve una dirección aleatoria del viento (uniforme).
    """
    direcciones = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
    return random.choice(direcciones)


# --- Función principal para simular una lectura completa ---
def generar_lectura():
    lectura = {
        "temperatura": sensor_temperatura(),
        "humedad": sensor_humedad(),
        "direccion_viento": sensor_viento()
    }
    return lectura


# --- Bucle continuo de simulación ---
if __name__ == "__main__":
    print("Simulando lecturas del sensor meteorológico...\n")
    while True:
        lectura = generar_lectura()
        print(json.dumps(lectura, ensure_ascii=False))
        time.sleep(3)  
