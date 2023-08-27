import Adafruit_DHT
import time
import json
import os
import datetime

from awscrt import mqtt
from awsiot import mqtt_connection_builder
from os.path import join, dirname
from dotenv import load_dotenv
from mqtt_callbacks import callbacks

dotenv_path = join(dirname(__file__), './.env')
load_dotenv(dotenv_path)

DHT_SENSOR = Adafruit_DHT.DHT22
DHT_PIN = os.getenv("DHT_PIN")
MESSAGE_DELAY_IN_SEC = 300

INPUT_ENDPOINT = os.getenv("INPUT_ENDPOINT")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
CLIENT_ID = os.getenv("CLIENT_ID")

CA_FILE = os.getenv("CA_FILE")
CERT = os.getenv("CERT")
KEY = os.getenv("KEY")


def publish_message(message, topic):
    print("Publishing message to topic '{}': {}".format(topic, message))
    message_json = json.dumps(message)
    mqtt_connection.publish(
        topic=topic,
        payload=message_json,
        qos=mqtt.QoS.AT_LEAST_ONCE)


if __name__ == '__main__':

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=INPUT_ENDPOINT,
        port=PORT,
        cert_filepath=CERT,
        pri_key_filepath=KEY,
        ca_filepath=CA_FILE,
        on_connection_interrupted=callbacks.on_connection_interrupted,
        on_connection_resumed=callbacks.on_connection_resumed,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=callbacks.on_connection_success,
        on_connection_failure=callbacks.on_connection_failure,
        on_connection_closed=callbacks.on_connection_closed)

    print("Connecting to endpoint with client ID {}".format(CLIENT_ID))
    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected!")

    try:
        while True:
            humidity, temperature = Adafruit_DHT.read_retry(DHT_SENSOR, DHT_PIN)

            if humidity is not None and temperature is not None:
                now = datetime.datetime.now()
                print("Temp={0:0.1f}*C  Humidity={1:0.1f}% Timestamp={2}".format(temperature, humidity, now))

                message = {
                    "temperature": temperature,
                    "humidity": humidity
                }
                publish_message(message, TOPIC)

            else:
                print("Failed to get data from sensor")

            time.sleep(MESSAGE_DELAY_IN_SEC)

    except Exception as e:
        print("Failed to send message via mqtt. Disconnecting. Cause: {}".format(str(e)))
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Disconnected!")
