"""
Name: Kristen Finley
Date: 9/14/2023

Work Queues Tutorial: https://www.rabbitmq.com/tutorials/tutorial-two-python.html

Modifies the send.py code from our previous example, 
to allow arbitrary messages to be sent from the command line. 

This program will schedule tasks to our work queue.



"""


#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()
