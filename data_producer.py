# Fetch stock price and send to kafka based on watchlist
# Fetch twitter and send to kafka based on watchlist
# Dynamically add/remove fetch thread upon watchlist change
import requests
import json
import logging
import argparse
import atexit
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import tweepy
from tweepy.auth import OAuthHandler
from tweepy.error import TweepError
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig()
logger = logging.getLogger("tweet_producer")
logger.setLevel(logging.DEBUG)

scheduler = BackgroundScheduler()
scheduler.add_executor("threadpool")
scheduler.start()

app = Flask(__name__)
app.config.from_envvar('CONFIG_FILE')
watchlist = set(["AAPL", "TSLA"])

kafka_broker = app.config["KAFKA_BROKER"]

producer = KafkaProducer(bootstrap_servers = kafka_broker)
consumer = KafkaConsumer(
		"watchlist",
		auto_offset_reset = "latest",
		enable_auto_commit = False,
		bootstrap_servers = kafka_broker)

class Listener(tweepy.StreamListener):
	def __init__(self, producer, symbol, api):   
		self.producer = producer
		self.symbol = symbol
		self.api = api
		super(tweepy.StreamListener, self).__init__()    

	def on_status(self, status):
		if status.retweeted or status.text[:4] == "RT @":
			return True   

		print(status.text)

		try:
			self.producer.send(
				topic=self.symbol + "_tweet", 
				value=status.text.encode('utf-8'), 
				timestamp_ms=time.time())
		except KafkaTimeoutError as te:
			logger.warn("Failed to send tweet caused by: %s", te.message)
			return False
		except Exception as e:
			logger.warn("Failed to send tweet caused by: %s", str(e))
			return False
		return True

	def on_error(self, status_code):
		logger.warn("Error " + str(status_code))
		return True # To continue listening

	def on_timeout(self):
		logger.warn("Timeout")
		return True # To continue listening

def fetch_price(symbol):
	url = "https://api.iextrading.com/1.0/stock/" + symbol + "/quote"
	rsp = requests.get(url)
	try:
		producer.send(
			topic = symbol, 
			value = rsp.content, 
			timestamp_ms = int(time.time()))
	finally:
		logger.info("fetched %s stock price", symbol)

def tweepy_auth():
	# Set up twitter API authentication
	consumer_key = app.config["CONSUMER_KEY"]
	consumer_secret = app.config["CONSUMER_SECRET"]
	access_token = app.config["ACCESS_TOKEN"]
	access_secret = app.config["ACCESS_SECRET"]
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	return tweepy.API(auth)

@app.route("/<symbol>", methods=["POST"])
def on_watchlist_add(symbol):
	watchlist.add(symbol)
	scheduler.add_job(fetch_price, "interval", [producer, symbol], seconds = 10, id = symbol)

@app.route("/<symbol>", methods=["DELETE"])
def on_watchlist_remove(symbol):
	watchlist.remove(symbol)
	scheduler.remove_job(symbol)

def shutdown_hook():
	try:
		logger.info("Flushing pending messages to kafka")
		producer.flush(10)	
	except KafkaError as ke:
		logger.warn("Failed to flush:\n%s", ke.message)
	finally:
		try:
			logger.info("Terminating kafka connection")
			producer.close(10)
		except Exception as e:
			logger.error("Failed to terminate kafka connection:\n%s", str(e))

	try:
		logger.info("Shutting down scheduler")
		scheduler.shutdown()
	except Exception as e:
		logger.error("Failed to shutdown scheduler:\n%s", str(e))

def main():
	tw_api = tweepy_auth()
	atexit.register(shutdown_hook)

	for symbol in watchlist:
		#watchlist.add(symbol)
		#scheduler.add_job(fetch_price, "interval", [symbol], seconds = 5, id = symbol)
		try:
			listener = Listener(producer, symbol, tw_api)
			stream = tweepy.Stream(auth = tw_api.auth, listener = listener)
			stream.filter(track = ["$"+symbol], async = True, languages = ["en"])
		except TweepError as te:
			logger.debug("TweepyExeption: Failed to get tweet for stocks caused by: %s" % te.message)

	app.run(port = app.config["FLASK_APP_PORT"])

if __name__ == "__main__":
	main()