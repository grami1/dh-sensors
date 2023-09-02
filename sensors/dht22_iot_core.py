import Adafruit_DHT
import time
import json
import os
import datetime
import sys

from awscrt import mqtt
from awsiot import mqtt_connection_builder
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), './../.env')
load_dotenv(dotenv_path)

DHT_SENSOR = Adafruit_DHT.DHT22
DHT_PIN = os.getenv("DHT_PIN")
MESSAGE_DELAY_IN_SEC = 30
SENSOR_ID = 'dht22'

INPUT_ENDPOINT = os.getenv("INPUT_ENDPOINT")
PORT = int(os.getenv("PORT"))
TOPIC = os.getenv("TOPIC")
CLIENT_ID = os.getenv("CLIENT_ID")

CA_FILE = os.getenv("CA_FILE")
CERT = os.getenv("CERT")
KEY = os.getenv("KEY")


def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code,
                                                                                  callback_data.session_present))


def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailuredata)
    print("Connection failed with error code: {}".format(callback_data.error))


def on_connection_closed(connection, callback_data):
    print("Connection closed")


def publish_message(pub_message, topic):
    print("Publishing message to topic '{}': {}".format(topic, pub_message))
    message_json = json.dumps(pub_message)
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
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    print("Connecting to endpoint with client ID {}".format(CLIENT_ID))
    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected!")

    try:
        while True:
            humidity, temperature = Adafruit_DHT.read_retry(DHT_SENSOR, DHT_PIN)

            if humidity is not None and temperature is not None:
                now = datetime.datetime.now().isoformat()

                message = {
                    "sensorId": SENSOR_ID,
                    "temperature": "{0:0.1f}".format(temperature),
                    "humidity": "{0:0.1f}".format(humidity),
                    "timestamp": now
                }
                publish_message(pub_message=message, topic=TOPIC)

            else:
                print("Failed to get data from sensor")

            time.sleep(MESSAGE_DELAY_IN_SEC)

    except Exception as e:
        print("Failed to send message via mqtt. Disconnecting. Cause: {}".format(str(e)))
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Disconnected!")
