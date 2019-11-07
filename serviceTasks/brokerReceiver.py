import pika
import logging
import os
from multiprocessing import Process
import sys
sys.path.insert(1, 'C:\\Apps\\Analytics\\common')
import dataAccess as dtAccss
from analyticsEmail import sendEmail

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
        logging.warning('Listening to broker queue {} on {}'.format(self.queue, os.environ.get('ADUB_Host', 'e')))

    def callback(self, ch, method, properties, body):
        try:
            logging.warning("Received %r" % body)
            self.job(body)
        except Exception as e:
            logging.error('Broker Receiver error for queue {}: {}'.format(self.queue, str(e)))
            sendEmail('Error', 'Broker Receiver for {}'.format(self.queue), str(e))

    def stopConsuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self.channel:
            logging.warning('Sending a Basic.Cancel RPC command to RabbitMQ')
            
            self.channel.add_on_cancel_callback(self.on_cancelok)
            self.channel.basic_cancel(self.consumer_tag)

    def receive(self):
        credentials = pika.PlainCredentials('adub', 'adub')
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('ADUB_Host', 'e'),5672,'/',credentials))
            self.channel = connection.channel()

            self.channel.queue_declare(queue=self.queue)

            self.consumer_tag = self.channel.basic_consume(queue=self.queue,on_message_callback=lambda  ch, method, properties, body: self.callback(ch, method, properties, body), auto_ack=True)
            
            logging.warning(' [*] Waiting for messages. To exit press CTRL+C')
            self.channel.start_consuming()
        except Exception as e:
            logging.error('Broker Receiver error for queue {}: {}'.format(self.queue, str(e)))
            sendEmail('Error', 'Broker Receiver for {}'.format(self.queue), str(e))

    def run(self):   
        self.process = Process(target=self.receive)
        self.process.start()  