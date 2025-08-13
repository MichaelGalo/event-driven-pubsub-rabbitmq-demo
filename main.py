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
import pika

load_dotenv()
logger = setup_logging()

listened_folder = Path("./data")
EXCHANGE = "data_exchange"
RABBIT_URL = os.getenv("RABBITMQ_CONNECTION_STRING")

def publish_message(body: bytes) -> None:
    params = pika.URLParameters(RABBIT_URL)
    conn = pika.BlockingConnection(params)
    channel = conn.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout")
    channel.basic_publish(exchange=EXCHANGE, routing_key="", body=body)
    conn.close()
    logger.info("Published filename message: %s", body.decode(errors="ignore"))

def wait_for_file_ready(path: Path, timeout: float = 10.0, poll_interval: float = 0.1) -> bool:
    """Wait until file is readable and size is stable or timeout occurs."""
    deadline = time.time() + timeout
    last_size = -1
    while time.time() < deadline:
        try:
            size = path.stat().st_size
            if size > 0 and size == last_size:
                # size stable across polls -> assume ready
                return True
            last_size = size
        except FileNotFoundError:
            pass
        time.sleep(poll_interval)
    return False


class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            logger.info(f"File that was transferred into: {event.src_path}")
            logger.info(f"Waiting for file to be ready.")
            if wait_for_file_ready(Path(event.src_path)):
                logger.info(f"File is ready, publishing to RabbitMQ.")
                with open(event.src_path, 'rb') as f:
                    publish_message(f.read())

def fetch_api_data():
    api_data = []
    for i in range(10):
        response = requests.get(os.getenv("API_URL"))
        if response.status_code != 200:
            logger.error(f"Request {i}: Failed to fetch API data: {response.status_code}")
            continue
        api_data.append(response.json())
    logger.info(f"Jokes fetched successfully")
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
