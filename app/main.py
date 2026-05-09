import paho.mqtt.client as mqtt
import json


BROKER = "192.168.1.17"
TOPIC = "sensor/moisture"

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode()

    try:
        data = json.loads(payload)

        moisture = data.get("analogValue")
        timestamp = data.get("timestamp")

        print("----- SENSOR DATA -----", flush=True)
        print(f"Topic: {msg.topic}", flush=True)
        print(f"Moisture: {moisture}", flush=True)
        print(f"Timestamp: {timestamp}", flush=True)
        print("-----------------------", flush=True)

    except json.JSONDecodeError:
        print("Invalid JSON received")

client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, 1885, 60)

client.loop_forever()