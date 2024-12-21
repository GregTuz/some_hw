from confluent_kafka import Consumer

if __name__ == '__main__':
	config = {
		'bootstrap.servers': 'localhost:9092',
		'group.id': 'kafka-python-getting-started',
		'auto.offset.reset': 'earliest'
	}

	consumer = Consumer(config)

	topic = 'example_topic'
	consumer.subscribe([topic])
	data = []
	try:
		while True:
			msg = consumer.poll(3.0)
			if msg is None:
				print('Waiting for message...')
			elif msg.error():
				print(f'An error occured: {msg.error()}')
			else:
				print(f'Consumed event from topic - {msg.topic()}, key = {msg.key().decode("utf-8")}, value = {msg.value().decode("utf-8")}')
				data.append({msg.key().decode("utf-8"): msg.value().decode("utf-8")})
	except KeyboardInterrupt:
		pass
	finally:
		consumer.close()
	print(data)
