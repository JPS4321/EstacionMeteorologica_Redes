from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import json


BROKER = "iot.redesuvg.cloud:9092"
TOPIC = "22102"
GROUP_ID = "grupo1"


wind_names = {0: "N", 1: "NE", 2: "E", 3: "SE", 4: "S", 5: "SO", 6: "O", 7: "NO"}


def decode_data(payload):
    code = int.from_bytes(payload, byteorder="big")

    temp_scaled = (code >> 10) & 0x3FFF  # 14 bits
    hum = (code >> 3) & 0x7F             # 7 bits
    wind_code = code & 0x7               # 3 bits

    temp = temp_scaled / 100.0
    wind = wind_names[wind_code]
    return {"temperatura": temp, "humedad": hum, "direccion_viento": wind}

# ==================================================
# CREAR CONSUMER
# ==================================================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    group_id=GROUP_ID,
    auto_offset_reset="latest"
)

print(f"\n Escuchando datos codificados en el topic '{TOPIC}' desde {BROKER}\n")

# ==================================================
# VARIABLES PARA GRÁFICA
# ==================================================
temps, humes, times = [], [], []

plt.ion()
plt.show(block=False)
plt.style.use("seaborn-v0_8-darkgrid")

fig, ax = plt.subplots()
line_temp, = ax.plot([], [], "r-", label="Temperatura (°C)")
line_hume, = ax.plot([], [], "b-", label="Humedad (%)")
ax.set_xlim(0, 20)
ax.set_ylim(0, 120)
ax.set_xlabel("Lecturas")
ax.set_ylabel("Valor")
ax.set_title(" Telemetría Meteorológica (3 Bytes)")
ax.legend()

# ==================================================
# FUNCIÓN DE ACTUALIZACIÓN
# ==================================================
def update(frame):
    global temps, humes, times
    try:
        msg = next(consumer)
        data = decode_data(msg.value)
        temps.append(data["temperatura"])
        humes.append(data["humedad"])
        times.append(len(times) + 1)

        print(f" Bytes: {msg.value} → Decodificado: {data}")

        # mantener últimas 20 lecturas
        temps, humes, times = temps[-20:], humes[-20:], times[-20:]
        line_temp.set_data(times, temps)
        line_hume.set_data(times, humes)
        ax.set_xlim(0, max(20, len(times)))
        plt.pause(0.001)

    except StopIteration:
        pass
    return line_temp, line_hume

# ==================================================
# ANIMACIÓN EN TIEMPO REAL
# ==================================================
ani = FuncAnimation(fig, update, interval=3000, cache_frame_data=False)
plt.show(block=True)
