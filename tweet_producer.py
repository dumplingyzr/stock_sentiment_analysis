import json
import logging
import argparse
import atexit
import redis
from kafka import KafkaConsumer
from tweepy.auth import OAuthHandler
from tweepy.error import TweepError

redis_channel = 

def tweepy_auth(key_file):
	# Set up twitter API authentication
	twitter_key = json.loads(open(key_file))
	consumer_key = twitter_key["consumer_key"]
	consumer_secret = twitter_key["consumer_secret"]
	access_token = twitter_key["access_token"]
	access_secret = twitter_key["access_secret"]
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	return auth

def main():
	cfg = json.loads(open("configuration.json"))
	auth = tweepy("twitter_auth_key.json")
	kafka_broker = cfg["kafka_broker"]
	redis_host = cfg["redis_host"]
	redis_port = cfg["redis_port"]

	watchlist_consumer = KafkaConsumer(
		"watchlist", 
		bootstrap_servers=kafka_broker,
		auto_offset_reset="latest")




if __name__ == "__main__":
	main()