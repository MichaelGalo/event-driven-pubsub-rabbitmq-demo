import os
import pika

from dotenv import load_dotenv

from logger import setup_logging
logger = setup_logging()
load_dotenv()

RABBIT_URL = os.getenv("RABBITMQ_CONNECTION_STRING")
EXCHANGE   = "data_exchange"

def callback(ch, method, properties, body):
    try:
        filename = body.decode()
    except Exception:
        filename = str(body)
    logger.info(f"Received filename: {filename}")

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