#!/usr/bin/env python

#----------------------------------------------------------------------------------------
# authors, description, version
#----------------------------------------------------------------------------------------
    # Endre Eres
    # Simple dummy Kafka Producer-Consumer mutlithreaded example: dummy_kafka_pro_con.py
    # V.1.0.0.
#----------------------------------------------------------------------------------------

import time
import json
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer

class dummy_kafka_pro_con():

    def __init__(self, sock=None):

        self.spin()

    def producer1(self):
        def producer1_callback():
            producer1 = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            producer1.send('to_ros', {"op": "advertise", "topic": "/chatter", "type": "std_msgs/String"})
            time.sleep(1)
            while (1):
                producer1.send('to_ros', {"op": "publish", "topic": "/chatter", "msg":{"data" : "KAFKA: Hello World"}})
                time.sleep(1)
        t = threading.Thread(target=producer1_callback)
        t.daemon = True
        t.start()
    
    def producer2(self):
        def producer2_callback():
            producer2 = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            producer2.send('to_ros', {"op": "advertise", "topic": "/chatter2", "type": "std_msgs/String"})
            time.sleep(1)
            while (1):
                producer2.send('to_ros', {"op": "publish", "topic": "/chatter2", "msg":{"data" : "KAFKA: Hello World 2"}})
                time.sleep(0.7)
        t = threading.Thread(target=producer2_callback)
        t.daemon = True
        t.start()

    def producer3(self):
        def producer3_callback():
            producer3 = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            producer3.send('to_ros', {"op": "subscribe", "topic": "/chatter", "type": "std_msgs/String"})
            time.sleep(1)
        t = threading.Thread(target=producer3_callback)
        t.daemon = True
        t.start()

    def producer4(self):
        def producer4_callback():
            producer4 = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            producer4.send('to_ros', {"op": "subscribe", "topic": "/chatter2", "type": "std_msgs/String"})
            time.sleep(1)
        t = threading.Thread(target=producer4_callback)
        t.daemon = True
        t.start()

    def producer5(self):
        def producer5_callback():
            producer5 = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))
            producer5.send('to_ros', {"op": "unsubscribe", "topic": "/chatter2"})
            time.sleep(1)
        t = threading.Thread(target=producer5_callback)
        t.daemon = True
        t.start()

    def consumer1(self):
        def consumer1_callback():
            consumer1 = KafkaConsumer('to_ros',
                                      bootstrap_servers='localhost:9092',
                                      value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                      auto_offset_reset='latest',
                                      consumer_timeout_ms=1000)

            while (1):
                for message in consumer1:
                    pass
                    #print (message.value)

        t = threading.Thread(target=consumer1_callback)
        t.daemon = True
        t.start()
    
    def consumer2(self):
        def consumer2_callback():
            consumer2 = KafkaConsumer('from_ros',
                                      bootstrap_servers='localhost:9092',
                                      value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                      auto_offset_reset='latest',
                                      consumer_timeout_ms=1000)

            while (1):
                for message in consumer2:
                    print (message.value)

        t = threading.Thread(target=consumer2_callback)
        t.daemon = True
        t.start()

    def spin(self):
        self.producer1()
        self.producer2()
        time.sleep(2)
        self.producer3()
        self.producer4()
        self.consumer1()
        self.consumer2()
        #time.sleep(10)
        #self.producer5()
        while (1):
            pass
            
  
if __name__ == '__main__':
    try:
        dummy_kafka_pro_con()
    except KeyboardInterrupt:
        pass