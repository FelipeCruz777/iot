import random
import time
from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import json

def criar_conexao_aws(endpointaws, portNumber, cerfilepath, prikey_path, cafile_path, clientid):
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpointaws,
        port=portNumber,
        cert_filepath=cerfilepath,
        pri_key_filepath=prikey_path,
        ca_filepath=cafile_path,
        client_id=clientid,
        clean_session=False,
        keep_alive_secs=30)

    return mqtt_connection

mqtt_connection = criar_conexao_aws('a2lqctm6m5tj9c-ats.iot.us-east-1.amazonaws.com', 8883, "Certificado.pem.crt", "private-private.pem.key",
                                        "AmazonRootCA1.pem", 'teste')

# Conectar ao AWS IoT Core
connect_future = mqtt_connection.connect()
connect_future.result()  # Aguardar a conexão ser estabelecida

# Enviar dados aleatórios de temperatura em loop
while True:
    temperatura = random.uniform(21, 30)
    humidity = random.uniform(40, 60)
    vibration = random.uniform(0, 1)
    noise = random.uniform(30, 70)
    proximity = random.uniform(0, 10)
    weight = random.uniform(1, 10)
    latitude = random.uniform(-90, 90)
    longitude = random.uniform(-180, 180)

    message_json = json.dumps({
        'temperature': temperatura,
        'humidity': humidity,
        'vibration': vibration,
        'noise': noise,
        'proximity': proximity,
        'weight': weight,
        'latitude': latitude,
        'longitude': longitude,
        'fk_truck': 1,
        'fk_route': 1
    })

    mqtt_connection.publish(
        topic="sdk/topico/cruz",
        payload=message_json,
        qos=mqtt.QoS.AT_LEAST_ONCE)
    
    print(f"Enviado: {message_json}")
    
    time.sleep(5)  # Esperar 5 segundos antes de enviar o próximo dado

# Desconectar do AWS IoT Core (isso nunca será alcançado no loop acima)
mqtt_connection.disconnect()
