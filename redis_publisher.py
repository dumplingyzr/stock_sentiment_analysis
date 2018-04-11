# read from Kafka and publish to web server

import redis
from kafka import KafkaConsumer
import logging
import atexit
from threading import Thread
from data_producer import app

logging.basicConfig()
logger = logging.getLogger("redis_publisher")
logger.setLevel(logging.DEBUG)

kafka_broker = app.config["KAFKA_BROKER"]
redis_host = app.config["REDIS_HOST"]
redis_port = app.config["REDIS_PORT"]

redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

#Place holder for tweet kafka consumer
tweet_consumer = None
redis_thread = None

def shutdown_hook():
	redis_thread.join()
	tweet_consumer.close()

class RedisPublisher(Thread):
	#daemon = True
	def __init__(self, redis_client, consumer):
		Thread.__init__(self)
		self.redis_client = redis_client
		self.consumer = consumer

	def run(self):
		for msg in self.consumer:
			logger.info("Publishing to web server: %s", msg.value)
			self.redis_client.publish("tweet", msg.value)

@app.route("/<symbol>", methods=["GET"])
def on_active_symbol(symbol):
	global redis_thread
	global tweet_consumer
	if redis_thread != None:
		redis_thread.exit()
		tweet_consumer.close()
	logger.info("Starting kafka consumer on %s", kafka_broker)
	tweet_consumer = KafkaConsumer(symbol + "_tweet", bootstrap_servers=kafka_broker)
	logger.info("Subscribing topic: %s_tweet", symbol)
	redis_thread = RedisPublisher(redis_client, tweet_consumer)
	redis_thread.start()

def main():
	on_active_symbol("aapl")
	atexit.register(shutdown_hook)

if __name__ == "__main__":
	main()