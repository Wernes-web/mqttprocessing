import paho.mqtt.client as mqtt
import json
import psycopg2




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



def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode()

    try:
        data = json.loads(payload)

        moisture = data.get("analogValue")
        timestamp = data.get("timestamp")

        cursor.execute(
            "INSERT INTO moisture_data (timestamp, moisture) VALUES (%s, %s)",
            (timestamp, moisture)
        )

        conn.commit()
        print("DB insert done", flush=True)

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