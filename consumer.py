from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# --- CONFIGURACIÓN DEL BROKER ---
BROKER = "iot.redesuvg.cloud:9092"
TOPIC = "22102"       # tu número de carné
GROUP_ID = "grupo1"       # puede ser cualquier nombre

# --- CREAR CONSUMER ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'  
)

print(f"Escuchando datos en el topic '{TOPIC}' desde {BROKER}...\n")

# --- LISTAS PARA ALMACENAR DATOS ---
temps = []
humes = []
winds = []
times = []

# --- CONFIGURAR GRÁFICO ---
plt.ion()  # activa modo interactivo
plt.show(block=False)  # evita que bloquee la ventana

plt.style.use('seaborn-v0_8-darkgrid')
fig, ax = plt.subplots()
line_temp, = ax.plot([], [], 'r-', label='Temperatura (°C)')
line_hume, = ax.plot([], [], 'b-', label='Humedad (%)')
ax.set_xlim(0, 20)
ax.set_ylim(0, 120)
ax.set_xlabel('Lecturas')
ax.set_ylabel('Valor')
ax.set_title(' Telemetría Meteorológica en Vivo')
ax.legend()

# --- FUNCIÓN PARA ACTUALIZAR EL GRÁFICO ---
def update(frame):
    global temps, humes, winds, times

    try:
        # leer un mensaje del broker (sin bloquear demasiado)
        mensaje = next(consumer)
        payload = mensaje.value

        temperatura = payload.get("temperatura")
        humedad = payload.get("humedad")
        direccion = payload.get("direccion_viento")

        temps.append(temperatura)
        humes.append(humedad)
        winds.append(direccion)
        times.append(len(times) + 1)

        print(f" Recibido: {payload}")

        # limitar a las últimas 20 lecturas
        if len(times) > 20:
            temps = temps[-20:]
            humes = humes[-20:]
            times = times[-20:]

        # actualizar gráfico
        line_temp.set_data(times, temps)
        line_hume.set_data(times, humes)
        ax.set_xlim(0, max(20, len(times)))

    except StopIteration:
        pass

    plt.pause(0.001)
    return line_temp, line_hume

# --- ANIMACIÓN EN TIEMPO REAL ---
ani = FuncAnimation(fig, update, interval=3000, cache_frame_data=False)
plt.show(block=True)
