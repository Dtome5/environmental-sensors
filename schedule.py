from datetime import datetime
import subprocess
from prefect import flow, task
from extract import load, update, check


@task()
def initial_load():
    load()


@task
def update_db():
    update()


@flow()
def schedule():
    if check == False:
        initial_load()
    else:
        update()


if __name__ == "__main__":
    schedule.serve(name="myflow", cron="0 * * * *")
