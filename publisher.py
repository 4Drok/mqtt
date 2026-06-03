import time
import random
import json
import paho.mqtt.client as mqtt

# Configuración del Broker
BROKER = "broker.hivemq.com"
PUERTO = 1883

def conectar_mqtt():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    print(f"Conectando al broker {BROKER}...")
    client.connect(BROKER, PUERTO, 60)
    return client

def main():
    cliente = conectar_mqtt()
    cliente.loop_start()
    
    # Lista de cámaras para simular el comportamiento intercalado
    camaras = ["camara_01", "camara_02"]
    indice_camara = 0
    contador_envios = 0

    try:
        while True:
            id_camara = camaras[indice_camara]
            # Construcción dinámica del tópico sin comodines para la publicación
            topico = f"unmsm/callao/camara/{id_camara}/telemetria"
            
            contador_envios += 1
            
            # 1. Definición del valor base por defecto (Temperatura normal de frío: < 5.0 °C)
            valor_temperatura = round(random.uniform(-5.0, 4.0), 2)
            
            # 2. Inyección intencional de anomalías e interrupciones
            if contador_envios % 5 == 0:
                # Caso A: Supera el umbral de la cadena de frío pero es físicamente válido para Pydantic
                valor_temperatura = round(random.uniform(5.5, 12.0), 2)
                print(f"[SIMULADOR] -> Provocando Alerta: Temperatura elevada en {id_camara}")
                
            elif contador_envios % 7 == 0:
                # Caso B: Falla de validación Pydantic (Fuera del rango estipulado de 100.0 °C)
                valor_temperatura = 150.0
                print(f"[SIMULADOR] -> Provocando Falla Pydantic: Valor fuera de límites en {id_camara}")
                
            elif contador_envios % 9 == 0:
                # Caso C: Falla crítica de tipo de dato (Se envía texto en vez de número float)
                valor_temperatura = "ERROR_ANOMALO"
                print(f"[SIMULADOR] -> Provocando Falla Pydantic: Tipo de dato string en {id_camara}")

            # Construcción del Payload estructurado
            datos_sensor = {
                "sensor_id": 901 if id_camara == "camara_01" else 902,
                "timestamp": time.time(),
                "valor": valor_temperatura,
                "unidad": "Celsius"
            }

            mensaje = json.dumps(datos_sensor)
            
            # Publicación garantizada mediante QoS 1
            info = cliente.publish(topico, mensaje, qos=1)
            info.wait_for_publish()
            
            print(f"[PUBLISHER] Enviado a {topico}: {mensaje}\n")
            
            # Alternar el índice para intercalar las cámaras en la siguiente iteración
            indice_camara = (indice_camara + 1) % len(camaras)
            time.sleep(3) # Frecuencia de muestreo de 3 segundos
            
    except KeyboardInterrupt:
        print("\nDeteniendo publicador del reto...")
    finally:
        cliente.loop_stop()
        cliente.disconnect()

if __name__ == "__main__":
    main()