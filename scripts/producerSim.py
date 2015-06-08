#!/usr/bin/env python
import time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime
 
kafka =  KafkaClient("localhost:9092")
 
producer = SimpleProducer(kafka)
 
#producer.send_messages("pythontest", "This is message sent from python client " + str(datetime.now().time()) 

#filename = "LICENSE" 
filename = 'data/bos_hous_labeled.csv'

with open(filename) as f:
	lines = f.readlines()
	
for line in lines[1:]:
	time.sleep(0.25)
	producer.send_messages("sparkml", line)