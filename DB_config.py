import pymongo
from sqlalchemy import create_engine
from config import POSTGRES_PASSWORD

# Create PostgreSQL engine
engine = create_engine(f'postgres://postgres:{POSTGRES_PASSWORD}@localhost:5432/Scooters_DB')
conn_postgreSQL = engine.connect()

# MongoDB
conn = 'mongodb://localhost:27017'
client_mongoDB = pymongo.MongoClient(conn)