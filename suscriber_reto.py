import json
import time
import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, ValidationError

# Definimos el esquema de datos según el diseño del laboratorio
class LecturaSensor(BaseModel):
    sensor_id: int
    timestamp: float
    valor: float = Field(..., ge=-50.0, le=100.0) # Límites físicos originales de validación
    unidad: str

BROKER = "broker.hivemq.com"
PUERTO = 1883
TOPICO_WILDCARD = "unmsm/callao/camara/+/telemetria"
ARCHIVO_LOG = "log_errores.txt"

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Conectado exitosamente al Broker MQTT")
        # Registro al tópico utilizando el comodín '+'
        client.subscribe(TOPICO_WILDCARD)
        print(f"Escuchando de manera dinámica en: {TOPICO_WILDCARD}\n")
    else:
        print(f"Error de conexión. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    raw_payload = msg.payload.decode()
    
    # Extraer de forma limpia el ID_CAMARA analizando el tópico en el que se publicó
    try:
        segmentos_topico = msg.topic.split('/')
        id_camara = segmentos_topico[3] # Posición del parámetro dinámico
    except IndexError:
        id_camara = "Desconocida"

    try:
        # 1. Intentar decodificar JSON y someterlo a las restricciones de Pydantic
        datos_json = json.loads(raw_payload)
        lectura = LecturaSensor(**datos_json)
        
        # 2. Si la validación es exitosa, evaluar la regla de negocio de cadena de frío (> 5.0 °C)
        if lectura.valor > 5.0:
            print(f"\033[91m[PELIGRO] ¡Pérdida de cadena de frío en Cámara {id_camara}! Valor actual: {lectura.valor} {lectura.unidad}\033[0m")
        else:
            print(f"[OK] Cámara {id_camara} operando estable. Temperatura: {lectura.valor} {lectura.unidad}")

    except (json.JSONDecodeError, ValidationError) as error_detectado:
        # 3. Intercepción de fallas de integridad de red: Guardar en log_errores.txt y descartar
        print(f"\033[93m[ALERTA DE SEGURIDAD] Datos inválidos en Cámara {id_camara}. Registrando incidencia...\033[0m")
        
        marca_tiempo = time.strftime('%Y-%m-%d %H:%M:%S')
        registro_error = (
            f"--- NUEVO EVENTO DE ERROR ({marca_tiempo}) ---\n"
            f"Tópico origen: {msg.topic}\n"
            f"Payload corrupto: {raw_payload}\n"
            f"Detalle Técnico: {str(error_detectado)}\n"
            f"--------------------------------------------------\n\n"
        )
        
        # Guardar de manera asíncrona (Añadiendo líneas al final del archivo)
        with open(ARCHIVO_LOG, "a", encoding="utf-8") as archivo_log:
            archivo_log.write(registro_error)

def main():
    cliente = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    cliente.on_connect = on_connect
    cliente.on_message = on_message

    cliente.connect(BROKER, PUERTO, 60)
    
    print("Iniciando Suscriptor Inteligente con Tolerancia a Fallos...")
    cliente.loop_forever()

    if __name__ == "__main__":
        main()