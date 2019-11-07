import pika

credentials = pika.PlainCredentials('adub', 'adub')
connection = pika.BlockingConnection(pika.ConnectionParameters('arctargoprod',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='eiaUpdate')

channel.basic_publish(exchange='',
                      routing_key='eiaUpdate',
                      body=str.encode('all'))
print(" [x] Sent test")