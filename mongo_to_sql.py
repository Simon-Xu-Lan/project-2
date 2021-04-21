import pandas as pd
import pymongo
import time
from sqlalchemy import create_engine
from config import POSTGRES_PASSWORD, POSTGRES_HEROKU_PASSWORD_1, POSTGRES_HEROKU_PASSWORD_2, ATLAS_PASSWORD


# Create PostgreSQL engine
postgres_local_path = f'postgres://postgres:{POSTGRES_PASSWORD}@localhost:5432/Scooters_DB'
postgres_heroku_path_1 = f'postgresql://bsapfqjhpjbnae:{POSTGRES_HEROKU_PASSWORD_1}@ec2-34-233-0-64.compute-1.amazonaws.com:5432/d8oqd54nu7g7qr'
postgres_heroku_path_2 = f"postgresql://acurubwqqcguqg:{POSTGRES_HEROKU_PASSWORD_2}@ec2-107-20-153-39.compute-1.amazonaws.com:5432/dd87jp2gm4bgdr"
engine = create_engine(postgres_heroku_path_2)
# MongoDB connection
# local_conn = 'mongodb://localhost:27017'
atlas_conn = f'mongodb+srv://simon:{ATLAS_PASSWORD}@cluster0.23jm7.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'


def clean_data_to_sql():
    """
    Retrieve data from MongoDB, clean the data, then save the cleaned data to PostgreSQL
    To recycle mongoDB space, delete these data from mongoDB after successfully saved these data into PosegreSQL, 
    return: None
    """

    # create retrieve time 
    retrieved_at = int(time.time())

    # ---------------------------
    # Retrieve data from MongoDB
    # Only retrieve data that the time of "saved_at" is before the time of retrieved_at/now
    with pymongo.MongoClient(atlas_conn) as client:
        db = client.scooters_DB
        collection = db.scooters
        try:
            data = collection.find({"saved_at": {"$lt": retrieved_at}})
        except Exception as e:
            print("An exception occurred: ", e)
        else:
            print("retrieved data from MongoDB")
        

    # ---------------------------
    # Using pandas to clean data
    df = pd.DataFrame(list(data))

    df = df[["company", "last_updated", "bike_id", "lat", "lon", "is_reserved", "is_disabled"]]
    df = df.dropna()
    df = df.drop_duplicates()

    # For records that only have last_updated different, Keep the records with minimum last_updated and drop others
    df = df.groupby(["company", "bike_id", "lat", "lon"]).min()
    df = df.reset_index()

    # Remove disabled scooters
    is_not_disabled = df["is_disabled"] == 0
    df = df[is_not_disabled]

    # Remove the records that lat or lon equals to zero
    is_lat_good = df["lat"] != 0
    df = df[is_lat_good]

    is_lon_good = df["lon"] != 0
    df = df[is_lon_good]

    # Remove is_disabled column
    df = df[["company", "last_updated", "bike_id", "lat", "lon", "is_reserved"]]

    # ---------------------------
    # Save data to PostgreSQL
    # with engine.connect() as connection:
    with engine.connect() as connection:
        try:
            df.to_sql(name='scooters', con=connection, if_exists='append', index=False)
        except Exception as e:
            is_saved_to_sql = False
            print("An exception occurred: ", e)
        else:
            is_saved_to_sql = True
            print("save to PostGres")

    # ---------------------------
    # Delete data from mongoDB
    if is_saved_to_sql:
        with pymongo.MongoClient(atlas_conn) as client:
            db = client.scooters_DB
            collection = db.scooters
            try: 
                data = collection.delete_many({"saved_at": {"$lt": retrieved_at}})
            except Exception as e:
                print("An exception occurred: ", e)
            else:
                print("Delete from MongoDB")

    return is_saved_to_sql

# def weather_mongo_to_sql():
#     # with pymongo.MongoClient(conn) as client:
#     with client_mongoDB as client:
#         db = client.scooters_DB
#         collection = db.weather
#         data = collection.find()

#     # Using pandas to clean data
#     df = pd.DataFrame(list(data))
#     last_saved = df["saved_at"].max()

#     new_df = df[["air_temp", "humidity", "visibility", "wind_speed", "last_updated", "tractid"]].copy()
#     df1 = new_df.dropna()
#     cleaned_df = df1.drop_duplicates()

#     # Save data to PostgreSQL
#     # with engine.connect() as connection:
#     with conn_postgreSQL as connection:
#         cleaned_df.to_sql(name='weather_records', con=connection, if_exists='append', index=False)

#     # After save data to postgreSQL, delete processed data from MongoDB, so don't get the same data processed repeatedly
#     # with pymongo.MongoClient(conn) as client:
#     with client_mongoDB as client:
#         db = client.scooters_DB
#         collection = db.weather
#         collection.delete_many({"save_at": {"$lte": last_saved}})

