import time
import random
import json
import paho.mqtt.client as mqtt

# Configuración del Broker Público de Pruebas
BROKER = "broker.hivemq.com"
PUERTO = 1883
TOPICO = "unmsm/fisi/cc/sensor/temperatura"

def conectar_mqtt():
    # Inicializar cliente MQTT utilizando la API moderna v2
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    print(f"Conectando al broker {BROKER}...")
    client.connect(BROKER, PUERTO, 60)
    return client

def main():
    cliente = conectar_mqtt()
    cliente.loop_start() # Iniciar el bucle de red de fondo
    try:
        while True:
            # Generar datos simulados del sensor
            temperatura = round(random.uniform(15.0, 35.0), 2)
            datos_sensor = {
                "sensor_id": 404,
                "timestamp": time.time(),
                "valor": temperatura,
                "unidad": "Celsius"
            }
            # Serializar diccionario a JSON string
            mensaje = json.dumps(datos_sensor)
            # Publicar el mensaje con QoS 1 (Asegurar entrega)
            info = cliente.publish(TOPICO, mensaje, qos=1)
            info.wait_for_publish() # Bloquear hasta asegurar el envío
            print(f"[PUBLISHER] Enviado a {TOPICO}: {mensaje}")
            time.sleep(3) # Esperar 3 segundos
    except KeyboardInterrupt:
        print("\nDeteniendo publicador...")
    finally:
        cliente.loop_stop()
        cliente.disconnect()

if __name__ == "__main__":
    main()