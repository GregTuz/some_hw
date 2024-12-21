from random import choice
from confluent_kafka import Producer

def delivery_callback(err, msg):
	if err:
		print(f'An error occured: {msg.error()}')
	else:
		print(f'Produced an event to topic - {msg.topic()}, key - {msg.key().decode("utf-8")}, value - {msg.value().decode("utf-8")}')

if __name__ == '__main__':

	config = {
		'bootstrap.servers': 'localhost:9092'
	}

	producer = Producer(config)
	topic = 'primer'
	user = ['black_nigga', 'gay_nigga', 'fuck_nigga']
	product = ['crack', 'drugs', 'kfc']

	count = 0
	for _ in range(10):
		user_id = choice(user)
		prod = choice(product)
		producer.produce(topic=topic, prod, user_id, callback=delivery_callback())
		count += 1

	producer.poll(10000)
	producer.flush()