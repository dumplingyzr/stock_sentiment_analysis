# Get stock price through alpha vantage API
# Send stock price to Kafka

from kafka import KafkaProducer
from apscheduler.schedulers.background import BackgroundScheduler

import requests
import logging
import argparse
import atexit
import time

logging.basicConfig()
logger = logging.getLogger('stock-price-producer')
logger.setLevel(logging.DEBUG)

scheduler = BackgroundScheduler()
scheduler.add_executor('threadpool')
scheduler.start()


#alpha_vantage_key = "A08K764106Q275TP"
kafka_topic = "stock_price"
kafka_broker = "localhost:9092"

def shutdown_hook(producer):
	try:
		producer.flush(10)
	finally:
		scheduler.shutdown()
		producer.close(10)


def fetch_price(symbol, producer):
	url = "https://api.iextrading.com/1.0/stock/" + symbol + "/quote"
	rsp = requests.get(url)
	try:
		producer.send(topic=kafka_topic, 
			value=rsp.content, 
			timestamp_ms=int(time.time()))
	finally:
		logger.info("fetched %s stock price", symbol)


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--symbol', help = 'Stock Symbol')

	args = parser.parse_args()
	symbol = args.symbol

	producer = KafkaProducer(bootstrap_servers = kafka_broker)
	atexit.register(shutdown_hook, producer)
	fetch_price(symbol, producer)
	scheduler.add_job(fetch_price, 'interval', [symbol, producer], seconds = 1)

	while True:
		pass

if __name__ == '__main__':
	main()