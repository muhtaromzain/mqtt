from configparser import ConfigParser
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
import time
import psycopg2 as pydb
import json
from datetime import datetime

def getConfig():
    config_parser = ConfigParser()
    config_parser.read('config.ini') 
    return config_parser

def dbConn():
    config = getConfig()
    connection = pydb.connect(
    host = config['SERVER_DB']['host'],
    database = config['SERVER_DB']['database'],
    user = config['SERVER_DB']['user'],
    password = config['SERVER_DB']['password']
    )
    return connection

if __name__ == '__main__':

    config = getConfig()
    kafkaServer = dict(config['SERVER_KAFKA'])
    kafkaServer.update(config['GROUP_KAFKA'])
    consumer = Consumer(kafkaServer)

    conn = dbConn()

    cur = conn.cursor()

    # selected_offset = 19;
    # Set up a callback to handle the '--reset' flag.
    # def reset_offset(consumer, partitions):
    #     for p in partitions:
    #         p.offset = selected_offset
    #         # p.offset = OFFSET_BEGINNING
    #     print('assign', partitions)
    #     consumer.assign(partitions)

    # Subscribe to topic
    topic = "esp"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                # print(data)

                print("Device ID: {}".format(data['device_id']))
                print("Time: {}".format(data['time']))

                # time = datetime.strftime(data['time'], '%Y-%m-%d %H:%M:%S.%f')
                # print("Time (Converted): {}".format(time))

                print("Latitude: {}".format(data['loc_lat']))
                print("Longitude: {}".format(data['loc_long']))
                print("Fuel: {}".format(data['fuel']))
                print("Electric: {}".format(data['electric']))
                print("Is Electric: {}".format(data['is_electric']))
                print("Door Open: {}".format(data['door_open']))
                print("Temperature: {}".format(data['temperature']))
                print("Humidity: {}".format(data['humidity']))

                time = datetime.strptime(data['time'], '%Y-%m-%d %H:%M:%S.%f%z')

                sql = """INSERT INTO records VALUES ({}, %(timestamp)s, {}, {}, {}, {}, {}, {}, {}, {})""".format(
                data['device_id'],
                data['loc_lat'], 
                data['loc_long'], 
                data['fuel'],
                data['electric'],
                data['is_electric'],
                data['door_open'],
                data['temperature'],
                data['humidity']
                )

                # sql = """INSERT INTO records VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})""".format(
                # data['device_id'],
                # data['time'],
                # data['loc_lat'], 
                # data['loc_long'], 
                # data['fuel'],
                # data['electric'],
                # data['is_electric'],
                # data['door_open'],
                # data['temperature'],
                # data['humidity']
                # )

                cur.execute(sql, {'timestamp': time})
                # cur.execute(sql)

                conn.commit()
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()