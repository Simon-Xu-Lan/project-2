# Dependencies
import time
from flask import Flask, render_template, redirect, jsonify, request
from flask_cors import CORS
from flask_apscheduler import APScheduler
from scooters import get_scooter_data
# from apscheduler.schedulers.blocking import BlockingScheduler

from mongo_to_sql import clean_data_to_sql
from retrieve_data import get_all, get_by_name, get_latest

from sqlalchemy import create_engine
from config import POSTGRES_HEROKU_PASSWORD_2
# Create PostgreSQL engine
postgres_heroku_path_2 = f"postgresql://acurubwqqcguqg:{POSTGRES_HEROKU_PASSWORD_2}@ec2-107-20-153-39.compute-1.amazonaws.com:5432/dd87jp2gm4bgdr"
engine = create_engine(postgres_heroku_path_2)



app = Flask(__name__)
scheduler = APScheduler()

# Set global variable count with initial value as 0
total_runs = 0
count_get_scooter_data = 0
count_clean_data_to_sql = 0

def job1():
    global scheduler, total_runs, count_get_scooter_data, count_clean_data_to_sql
    total_runs += 1
    print(total_runs)
    get_scooter_data()
    count_get_scooter_data += 1
    is_successful = True
    if count_get_scooter_data == 60:
        is_successful = clean_data_to_sql()
        count_get_scooter_data = 0
        count_clean_data_to_sql += 1
    if not is_successful:
        scheduler.remove_job("scooter_job")
    print("job 1")



@app.route("/")
def home():
    return render_template("index.html", count=total_runs)

@app.route("/api/all", methods=["GET"])
def get_all_records():
    data = get_all()
    return jsonify(data=data)

@app.route("/api/company/<name>/<limit>")
def get_by_company_name(name, limit):
    return jsonify(data=get_by_name(name, limit))

@app.route("/api/latest")
def get_latest_records():
    return jsonify(data=get_latest())



if __name__ == '__main__':
    scheduler.add_job(func=job1, trigger="interval", id="scooters_job", seconds=60)
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()
    app.run()


