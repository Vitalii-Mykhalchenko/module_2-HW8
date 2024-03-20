import pika
import json
from mongoengine import connect, Document, StringField, BooleanField

mongo_uri = "mongodb+srv:"
connect(host=mongo_uri)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')

class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True, unique=True)
    message_sent = BooleanField(default=False)

def callback(ch, method, properties, body):
    email_data = json.loads(body)

    contact = Contact.objects(email=email_data['email']).first()
    if contact:
        contact.message_sent = True
        contact.save()

    print(" [x] Sent email to %r" % email_data['email'])


# Встановлення обробника повідомлень
channel.basic_consume(queue='email_queue',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
