import paho.mqtt.client as mqtt
from confluent_kafka import Producer, KafkaError
import json
from configparser import ConfigParser

config_parser = ConfigParser()
config_parser.read('config.ini') 
config = dict(config_parser['SERVER_KAFKA'])
producer = Producer(config)

# Create topic if needed
topic = "esp"

delivered_records = 0

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("esp")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))
    # print(msg.payload)
    # record_key = "alice"
    record_value = msg.payload
    # print("Producing record: {}\t{}".format(record_key, record_value))
    producer.produce(topic, value=record_value, on_delivery=acked)
    # p.poll() serves delivery reports (on_delivery)
    # from previous produce() calls.
    producer.poll(0)

    producer.flush()

    # print("{} messages were produced to topic {}!".format(delivered_records, topic))

def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
# client.tls_set()
client.username_pw_set(username="pentagon", password="area69")
print("Connecting....")
client.connect("demo.sbumedan.co.id", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()