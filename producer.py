import pika
import json
from faker import Faker
from mongoengine import connect, Document, StringField, BooleanField


mongo_uri = "mongodb+srv:"
connect(host=mongo_uri)

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost', port=5672, credentials=credentials))
channel = connection.channel()


class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True, unique=True)
    message_sent = BooleanField(default=False)


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.queue_declare(queue='email_queue')


def generate_and_send_contacts(num_contacts):
    fake = Faker()
    for _ in range(num_contacts):
        full_name = fake.name()
        email = fake.email()

        contact = Contact(full_name=full_name, email=email)
        contact.save()

        # Надсилання ObjectID контакту у чергу RabbitMQ
        email_data = {
            'contact_id': str(contact.id),
            'email': email
        }
        channel.basic_publish(exchange='',
                              routing_key='email_queue',
                              body=json.dumps(email_data))
        print(" [x] Sent email to %r" % email)


if __name__ == '__main__':
    generate_and_send_contacts(5)
    connection.close()
