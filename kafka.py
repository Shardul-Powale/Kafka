from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create a Kafka topic
def create_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])

# Producer
def produce_messages(topic_name):
    producer_config = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_config)

    for i in range(10):
        message = f"Message {i}"
        producer.produce(topic_name, value=message)
        producer.flush()

    producer.close()

# Consumer
def consume_messages(topic_name):
    consumer_config = {'bootstrap.servers': bootstrap_servers, 'group.id': 'my-group', 'auto.offset.reset': 'earliest'}
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue

        print('Received message: {}'.format(message.value().decode('utf-8')))

    consumer.close()

# Example usage
topic_name = 'shardul_topic'

# Create the topic (only needed once)
create_topic(topic_name)

# Start producing messages
produce_messages(topic_name)

# Start consuming messages
consume_messages(topic_name)
