import os
import pika
from sqlalchemy import create_engine
import pandas as pd

from dotenv import load_dotenv

from logger import setup_logging
logger = setup_logging()
load_dotenv()

RABBIT_URL = os.getenv("RABBITMQ_CONNECTION_STRING")
EXCHANGE   = "data_exchange"
WATCHED_FOLDER = os.getenv("WATCHED_FOLDER_PATH")
PG_CONN_STR = os.getenv("POSTGRES_CONNECTION_STRING")

def write_csv_to_postgres(filename):
    filepath = os.path.join(WATCHED_FOLDER, filename)
    df = pd.read_csv(filepath)
    engine = create_engine(PG_CONN_STR)
    df.to_sql('new_hires', engine, if_exists='append', index=False)
    engine.dispose()

def callback(ch, method, properties, body):
    try:
        filename = body.decode()
        logger.info(f"Received filename: {filename}.")
    except Exception:
        filename = str(body)
        logger.error(f"Failed to decode message body: {body}")
    
    try:
        logger.info(f"Attempting to upload to PostgreSQL: {filename}.")
        write_csv_to_postgres(filename)
    except Exception as e:
        logger.error(f"Failed to upload {filename} to PostgreSQL: {e}")

def main():
    params = pika.URLParameters(RABBIT_URL)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')
    q      = ch.queue_declare(queue='', exclusive=True).method.queue
    ch.queue_bind(exchange=EXCHANGE, queue=q)
    ch.basic_consume(queue=q, on_message_callback=callback, auto_ack=True)
    print("Subscriber1 waiting...")
    ch.start_consuming()

if __name__ == "__main__":
    main()