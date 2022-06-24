#!/usr/bin/env python

from configparser import ConfigParser
from signal import signal, SIGINT
from sys import exit
from random import randint
import time
from datetime import datetime
import json
import psutil
import pytz
import random
from paho.mqtt import client as mqtt_client

# connect to broker
def connect_mqtt(broker, port, client_id, username, password):

    # connect callback
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    
    # set connectiong client id
    client = mqtt_client.Client(client_id, clean_session=True, userdata=None, protocol=mqtt_client.MQTTv311, transport="tcp")
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# publishing messages
def publish(client, topic):
    

    # loping
    while True:

        # random number
        # temperature = randint(1, 100)
        cpu_percent = psutil.cpu_percent(interval=1)

        # time.sleep(1)
        # msg = f"cpu_load: {cpu_percent}"
        currentDate = datetime.now()
        timezone = pytz.timezone('Asia/Jakarta')
        currentDate = timezone.localize(currentDate)
        randBool = bool(random.getrandbits(1))
        randVal = random.uniform(50.0, 70.0)
        msg = json.dumps({
                    'device_id': '2',
                    'time': currentDate.isoformat(' '),
                    'loc_lat': 40.689247,
                    'loc_long': -74.044502,
                    'fuel': 0,
                    'electric': randVal,
                    'is_electric': True,
                    'door_open': randBool,
                    'temperature': cpu_percent,
                    'humidity': 0
                })
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")

# handler gracefully
def handler(signal_received, frame):
    # handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    exit(0)

# read config file
def getConfig():
    config = ConfigParser()
    config.read('config.ini')
    return config


def run():

    # handler
    signal(SIGINT, handler)

    while True:
        # get config
        config = getConfig()
        broker = config['SERVER']['broker']
        port = config['SERVER']['port']
        topic = config['SERVER']['topic']
        client_id = config['PUB']['client_id']
        user = config['USER']['username']
        paswd = config['USER']['password']

        client = connect_mqtt(broker, int(port), client_id, user, paswd)
        client.loop_start()
        publish(client, topic)
        pass

if __name__ == '__main__':
    run()
