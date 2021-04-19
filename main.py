# Dependencies
import time
from scooters import get_scooter_data
from apscheduler.schedulers.blocking import BlockingScheduler
# from weather import weather_data
# from mongo_to_sql import scooters_mongo_to_sql, weather_mongo_to_sql

# Set global variable count with initial value as 0
count = 1

def job():
    global scheduler, count
    print(count)
    get_scooter_data()
    if count == 3:
        scheduler.remove_job("scooters")
    count += 1
        

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(job, "interval", id="scooters", seconds=60)
    scheduler.start()


