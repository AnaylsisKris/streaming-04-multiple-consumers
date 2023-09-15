"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Kristen Finley
    Date: September 14, 2023

"""

import pika
import sys
import os
import csv
import time
import logging
import webbrowser

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)


# Declare program constants (typically constants are named with ALL_CAPS)

INPUT_FILE_NAME = "tasks.csv"
SHOW_OFFER = False  # Turn off RabbitMQ Admin webpage question


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()


def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)


        # Create a file object for input (r = read access)
        with open(INPUT_FILE_NAME, "r") as input_file:
            logger.info(f"Opened for reading: {INPUT_FILE_NAME}.")

            # Create a CSV reader object and sends to queue
            reader = csv.reader(input_file, delimiter=",")
            for row in reader:
                message = str(row)
                # use the channel to publish a message to the queue
                # every message passes through an exchange
                ch.basic_publish(exchange="", routing_key=queue_name, body=message)
                # log message for the user
                logger.info(f" [x] Sent {message}")
                # Wait 3 seconds between each message
                time.sleep(3)

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


 
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    if SHOW_OFFER == True:
        # ask the user if they'd like to open the RabbitMQ Admin site
        offer_rabbitmq_admin_site()

    # get the message from the command line
    # if no arguments are provided, use the default message
    # send the message to the queue
    send_message("localhost","task_queue3", "tasks.csv")