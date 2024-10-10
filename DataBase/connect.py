from pymongo import MongoClient

from BluePrints.crashes_bp import crash_bp

client = MongoClient('mongodb://localhost:27017/')
crash_db = client['Chicago_Car_Accidents']

crashes = crash_db['crashs']

