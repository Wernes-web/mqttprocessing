import paho.mqtt.client as mqtt
import json
import psycopg2
import time



BROKER = "192.168.1.17"
TOPIC = "sensor/moisture"

conn = psycopg2.connect(
    host="postgres",
    database="sensordb",
    user="postgres",
    password="password123"
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS moisture_data (
    id SERIAL PRIMARY KEY,
    timestamp TEXT,
    moisture INTEGER
)
""")

conn.commit()

last_insert_time = 0

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    global last_insert_time


    try:
        data = json.loads(payload)

        moisture = data.get("analogValue")
        timestamp = data.get("timestamp")


        current_time = time.time()

        if current_time - last_insert_time >= 300:

            cursor.execute(
                "INSERT INTO moisture_data (timestamp, moisture) VALUES (%s, %s)",
                (timestamp, moisture)
            )

            conn.commit()

            last_insert_time = current_time

            print("Stored data in PostgreSQL", flush=True)

        else:
            print("Skipping DB insert (waiting 5 min)", flush=True)


        print("----- Sensor Data  -----", flush=True)
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