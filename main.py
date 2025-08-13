#!/usr/bin/env python3
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from logger import setup_logging
import requests
import os
from dotenv import load_dotenv
import csv

load_dotenv()
logger = setup_logging()

listened_folder = Path("./data")

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"File that was transferred into: {event.src_path}")

def fetch_api_data():
    api_data = []
    for i in range(10):
        response = requests.get(os.getenv("API_URL"))
        if response.status_code != 200:
            logger.error(f"Request {i}: Failed to fetch API data: {response.status_code}")
            continue
        logger.info(f"Joke: {response.json().get('value')}")
        api_data.append(response.json())
    return api_data

def write_to_csv(data, file_path):
    with open(file_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def main():
    chuck_norris_api_data = "chuck_norris_jokes.csv"
    api_data = fetch_api_data()
    write_to_csv(api_data, listened_folder / chuck_norris_api_data)

    observer = Observer()
    event_handler = FileHandler()
    observer.schedule(event_handler, path=listened_folder, recursive=False)
    observer.start()
    logger.info(f"Started watching directory: {listened_folder}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopped watching due to keyboard interrupt.")
    observer.join()

if __name__ == "__main__":
    main()
