# define supporting classes used in this project such as scheduler and logger
import logging
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig()

def getScheduler():
	scheduler = BackgroundScheduler()
	scheduler.add_executor("threadpool")
	return scheduler

def getLogger(name):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	return logger