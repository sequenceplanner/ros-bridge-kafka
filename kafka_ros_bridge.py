#!/usr/bin/env python

#----------------------------------------------------------------------------------------
# authors, description, version
#----------------------------------------------------------------------------------------
    # Endre Eres
    # ROSBridge-Kafka multiclient: kafka_ros_bridge.py
    # V.0.1.0.
#----------------------------------------------------------------------------------------

import time
import json
import threading
import socket
from std_msgs.msg import String
from kafka import KafkaProducer
from kafka import KafkaConsumer

class Bridge():

    def __init__(self, sock=None):

        if sock == None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

        self.spin()

    def consumer_to_ros(self):
        def consumer_to_ros_callback():
            consumer = KafkaConsumer('to_ros',
                                      bootstrap_servers='localhost:9092',
                                      value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                      auto_offset_reset='latest',
                                      consumer_timeout_ms=1000)

            HOST = "localhost"
            PORT = 9090 
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((HOST, PORT))

            while (1):
                for message in consumer:
                    if message.value['op'] != "subscribe":
                        #print (message.value)
                        s.send(json.dumps(message.value))
                    else:
                        pass
                
        t = threading.Thread(target=consumer_to_ros_callback)
        t.daemon = True
        t.start()

    def producer_from_ros(self):
        def producer_from_ros_callback():
            consumer = KafkaConsumer('to_ros',
                                      bootstrap_servers='localhost:9092',
                                      value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                      auto_offset_reset='latest',
                                      consumer_timeout_ms=1000)

            while (1):
                for message in consumer:
                    if message.value['op'] == "subscribe" or message.value['op'] == "unsubscribe":
                        self.connect('localhost', 9090)
                        time.sleep(0.3)
                        self.send(message.value)
                        time.sleep(0.3)
                        self.receive()

        t = threading.Thread(target=producer_from_ros_callback)
        t.daemon = True
        t.start()

    def connect(self, host, port):
        def connect_callback():
            TCP_IP = 'localhost'
            TCP_PORT = 9090
            self.sock.connect((TCP_IP, TCP_PORT))
        t = threading.Thread(target=connect_callback)
        t.daemon = True
        t.start()
   
    def send(self, msg):
        def send_callback():
            self.sock.send(json.dumps(msg))
        t = threading.Thread(target=send_callback)
        t.daemon = True
        t.start()

    def receive(self):
        def receive_callback():
            producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            while (1):
                data = self.sock.recv(4096)
                if not data: break
                producer.send('from_ros', data)
                print data
            self.sock.close()
        t = threading.Thread(target=receive_callback)
        t.daemon = True
        t.start()

    def spin(self):
        self.consumer_to_ros()
        self.producer_from_ros()
        while (1):
            pass
 
if __name__ == '__main__':
    try:
        Bridge()
    except KeyboardInterrupt:
        pass