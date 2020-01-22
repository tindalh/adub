import pika
import logging
import os
from multiprocessing import Process
import sys
import helpers.dataAccess as dtAccss
from helpers.log import log

class BrokerReceiver(object):
    """An object that listens to a RabbitMQ queue"""
    def __init__(self, queue, database, job):
        self.queue = queue
        self.server = os.environ.get('ADUB_DBServer', 'e')
        self.database = database
        self.consumer_tag = None
        self.channel = None
        self.job = job
        self.connection = None
        self.processes = list()
        log(__name__, '__init__', f"Listening to broker queue {self.queue} on {os.environ.get('ADUB_Host', 'e')}")

    def callback(self, ch, method, properties, body):
        try:
            log(__name__, 'callback', f"{self.queue} received {body}")
            self.job(body)
        except Exception as e:
            log(__name__, 'callback', f"Broker Receiver error for queue {self.queue}: {str(e)}", 'Error', True, 'Import Watcher')

    def stopConsuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self.channel:
            
            self.channel.add_on_cancel_callback(self.on_cancelok)
            self.channel.basic_cancel(self.consumer_tag)

    def receive(self):
        credentials = pika.PlainCredentials('adub', 'adub')
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('ADUB_Host', 'e'),5672,'/',credentials))
            self.channel = connection.channel()

            self.channel.queue_declare(queue=self.queue)

            self.consumer_tag = self.channel.basic_consume(queue=self.queue,on_message_callback=lambda  ch, method, properties, body: self.callback(ch, method, properties, body), auto_ack=True)
            
            log(__name__, 'receive', f"[*] Waiting for messages on {self.queue}")
            self.channel.start_consuming()
        except Exception as e:
            log(__name__, 'receive', f"Broker Receiver error for queue {self.queue}: {str(e)}", 'Error', True, 'Import Watcher')

    def run(self):   
        self.process = Process(target=self.receive)
        self.process.start()  