# Dependencies
# Python libraries
import requests
import pymongo
import time

# Customized modules
from Resources.companies import compInfo
from geo_polygons import Polygons
from config import ATLAS_PASSWORD


# MONGODB
# Initialize PyMongo to work with MongoDBs
# conn = 'mongodb://localhost:27017'
# MongoDB atlas connection
atlas_conn = f'mongodb+srv://simon:{ATLAS_PASSWORD}@cluster0.23jm7.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'

client = pymongo.MongoClient(atlas_conn)

# Define database and collection
db = client.scooters_DB
collection = db.scooters1

# Create a Polygons instance/object
# polygons = Polygons()

# Initialize recent last_updated data
recent_last_updated = {}
for company in compInfo:
    recent_last_updated[company["name"]] = 0


# FUNCTIONS
def get_scooter_data():
    for company in compInfo:
        if company["is_chosen"]:
            name = company["name"]

            print("$$$ ", name)

            try:
                response = requests.get(company["url"])
                data = response.json()
            except:
                # If a company url doesn't work, continue next company url, print the url that is not work
                print(f"{name} url has issue")
                continue

            # Assign the value of last_updated to varialbe "last_updated"
            # if data doesn't have last_updated attibute, use current time as "last_updated"
            # Make sure the last_updated is integer for future use
            try:
                last_updated = int(data["last_updated"])
            except:
                # Issue, using current time would save much more records, need modify later
                last_updated = int(time.time())

            # Retrieve data by layers defined in compInfo
            for layer in company["layers"]:
                data = data[layer]

            # If last_updated changes, save data to mongoDB
            print(f"recent{recent_last_updated[name]}")

            doc_sets = process_data(data, name, last_updated)
            if last_updated > recent_last_updated[name]:
                # Process data and save data to MongoDB
                collection.insert_many(doc_sets)
            

            # update recent_last_updated
            recent_last_updated[name] = last_updated


def process_data(data, name, last_updated):
    doc_sets = []
    for row in data:
        new_doc = {}
        new_doc["company"] = name
        new_doc["last_updated"] = last_updated
        new_doc["bike_id"] = row["bike_id"]
        new_doc["lat"] = row["lat"]
        new_doc["lon"] = row["lon"]
        new_doc["is_reserved"] = row["is_reserved"]
        new_doc["is_disabled"] = row["is_disabled"]
        new_doc["saved_at"] = int(time.time())
        doc_sets.append(new_doc)
    return doc_sets