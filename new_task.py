"""
Name: Kristen Finley
Date: 9/14/2023

Work Queues Tutorial: https://www.rabbitmq.com/tutorials/tutorial-two-python.html

Modifies the send.py code from our previous example, 
to allow arbitrary messages to be sent from the command line. 

This program will schedule tasks to our work queue.



"""

import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ))
print(f" [x] Sent {message}")
connection.close()