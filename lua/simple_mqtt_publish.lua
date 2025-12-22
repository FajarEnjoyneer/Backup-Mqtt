local mosq = require("mosquitto")
local socket = require("socket")

local MQTT_CLOUD_HOST  = "103.150.226.102"
local MQTT_CLOUD_PORT  = 1883
local MQTT_CLOUD_TOPIC = "upload/data/backup"

local client = mosq.new("simple_publisher_" .. os.time())

print("Connecting to " .. MQTT_CLOUD_HOST .. "...")
client:connect(MQTT_CLOUD_HOST, MQTT_CLOUD_PORT, 60)

client:loop_start()

-- Wait for connection to stabilize
socket.sleep(1)

local json_payload = '{"message": "Hello from simple lua script", "ts": "' .. os.date("%Y-%m-%d %H:%M:%S") .. '"}'

print("Publishing to " .. MQTT_CLOUD_TOPIC)
print("Payload: " .. json_payload)

local mid = client:publish(MQTT_CLOUD_TOPIC, json_payload, 1, false)

-- Wait to ensure message is sent (QoS 1 needs ack)
socket.sleep(2)

client:disconnect()
client:loop_stop()

print("Done.")
