from sqlalchemy import create_engine

from config import POSTGRES_HEROKU_PASSWORD_2

from decimal import *

# Create PostgreSQL engine
postgres_heroku_path_2 = f"postgresql://acurubwqqcguqg:{POSTGRES_HEROKU_PASSWORD_2}@ec2-107-20-153-39.compute-1.amazonaws.com:5432/dd87jp2gm4bgdr"
engine = create_engine(postgres_heroku_path_2)

def to_dict(data):
    dicts = []
    for row in list(data):
        row = list(row)
        dictionary = {}
        dictionary["company"] = row[1]
        dictionary["last_updated"] = row[2]
        dictionary["scooter_id"] = row[3]
        dictionary["lat"] = float(row[4]) * 1.0
        dictionary["lon"] = float(row[5]) * 1.0
        dictionary["is_reserved"] = row[6]
        dicts.append(dictionary)
    
    return dicts

def get_all():
    data = engine.execute("SELECT * FROM scooters;")
    return to_dict(data)
    

def get_by_name(name, limit):
    data = engine.execute(f"SELECT * FROM scooters WHERE company = '{name}' LIMIT {limit}")
    return to_dict(data)

def get_latest():
    data = engine.execute("""
    SELECT * FROM scooters AS a
    WHERE last_updated in (
            SELECT min(last_updated) FROM scooters WHERE company = 'spin') 
            AND a.company = 'spin'
	    OR last_updated in (
            SELECT min(last_updated) FROM scooters WHERE company = 'razor') 
            AND a.company = 'razor'
	    OR last_updated in (
            SELECT min(last_updated) FROM scooters WHERE company = 'helbiz') 
            AND a.company = 'helbiz';
    """)

    return to_dict(data)
    
        # try:
        #     df.to_sql(name='scooters', con=connection, if_exists='append', index=False)
        # except Exception as e:
        #     is_saved_to_sql = False
        #     print("An exception occurred: ", e)
        # else:
        #     is_saved_to_sql = True
        #     print("save to PostGres")

