import paho.mqtt.client as mqtt
import json
import sqlite3
import time
import logging
from logging.handlers import RotatingFileHandler

log_file_path = 'backuppython.log'

# Configure logging with RotatingFileHandler
handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=3)  # 5 MB per file, keep 3 backups
logging.basicConfig(handlers=[handler], level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to connect to SQLite database
def connect_db():
    logger.debug("Connecting to main database: mqtt_data.db")
    try:
        conn = sqlite3.connect('mqtt_data.db')
        c = conn.cursor()
        # Create table to store data if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS mqtt_data
                     (timestamp INTEGER, id INTEGER, value1 INTEGER, value2 INTEGER, value3 INTEGER, value4 INTEGER, value5 INTEGER, value6 INTEGER, value7 INTEGER, value8 INTEGER)''')
        return conn, c
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database mqtt_data.db: {e}")
        return None, None

# Function to delete old data
def delete_old_data(c, conn):
    logger.debug("Deleting old data...")
    try:
        current_time = int(time.time())
        one_day_ago = current_time - 24*60*60  # 24 hours ago
        c.execute("DELETE FROM mqtt_data WHERE timestamp < ?", (one_day_ago,))
        conn.commit()
        logger.info("Deleted data older than 24 hours")
    except sqlite3.Error as e:
        logger.error(f"Error deleting old data: {e}")

# Callback function when connection is established
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe("test")  # Subscribe to the "test" topic
    else:
        logger.error(f"Failed to connect to MQTT broker with return code {rc}")

# Callback function when a message is received
def on_message(client, userdata, msg):
    logger.debug(f"Received message: {msg.payload}")
    try:
        # Parse the JSON data
        data = json.loads(msg.payload.decode('utf-8'))
        logger.info(f"Parsed data: {data}")

        # Extract values
        timestamp = data["ts"]
        id = data["id"]
        data_values = data["data"]

        # Break down the data values array
        value1, value2, value3, value4, value5, value6, value7, value8 = data_values

        # Connect to the main database
        conn, c = connect_db()
        if conn is None or c is None:
            logger.error("Failed to connect to the main database")
            return

        # Insert data into SQLite table
        c.execute("INSERT INTO mqtt_data (timestamp, id, value1, value2, value3, value4, value5, value6, value7, value8) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (timestamp, id, value1, value2, value3, value4, value5, value6, value7, value8))
        conn.commit()

        # Log message indicating data has been written to the database
        logger.info(f"Data written: {timestamp}, {id}")

        # Delete old data
        delete_old_data(c, conn)

    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON: {msg.payload}")
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

# Create an MQTT client instance
client = mqtt.Client()

# Assign callbacks
client.on_connect = on_connect
client.on_message = on_message

# Retry connection to the broker
connected = False
while not connected:
    try:
        logger.debug("Connecting to MQTT broker...")
        client.connect("127.0.0.1", 1883, 60)
        connected = True
    except Exception as e:
        logger.error(f"Error connecting to MQTT broker: {e}")
        time.sleep(10)  # Wait 10 seconds before retrying

# Loop to maintain connection and receive messages
try:
    logger.debug("Starting MQTT loop...")
    client.loop_forever()
except KeyboardInterrupt:
    logger.info("Script terminated by user")
    client.disconnect()
    exit(0)
except Exception as e:
    logger.error(f"Error in MQTT loop: {e}")
    client.disconnect()
    exit(1)
