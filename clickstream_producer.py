from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from faker import Faker
import json
import time
import random
import logging
import os
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)


CONFLUENT_CONFIG = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('SASL_MECHANISM'),
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'client.id': os.getenv('CLIENT_ID')
}

SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}


KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
SEND_INTERVAL_MIN = float(os.getenv('SEND_INTERVAL_MIN', 0.1))
SEND_INTERVAL_MAX = float(os.getenv('SEND_INTERVAL_MAX', 1.5))
USER_POOL_SIZE = int(os.getenv('USER_POOL_SIZE', 1000))
PRODUCTS_FILE = os.getenv('PRODUCTS_FILE', 'products.json')


CLICKSTREAM_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ClickstreamEvent",
  "description": "Clickstream event",
  "type": "object",
  "properties": {
    "user_id": { "type": ["null", "string"] },
    "session_id": { "type": ["null", "string"] },
    "demographics": {
      "type": ["null", "object"],
      "properties": {
        "age_group": { "type": ["null", "string"] },
        "gender": { "type": ["null", "string"] },
        "country": { "type": ["null", "string"] }
      }
    },
    "event_type": { "type": ["null", "string"] },
    "event_timestamp": { "type": ["null", "integer"] },
    "device": { "type": ["null", "string"] },
    "browser": { "type": ["null", "string"] },
    "os": { "type": ["null", "string"] },
    "page_url": { "type": ["null", "string"] },
    "referrer_url": { "type": ["null", "string"] },
    "product_details": {
      "type": ["null", "object"],
      "properties": {
        "id": { "type": ["null", "integer"] },
        "name": { "type": ["null", "string"] },
        "category": { "type": ["null", "string"] },
        "subcategory": { "type": ["null", "string"] },
        "price": { "type": ["null", "number"] },
        "brand": { "type": ["null", "string"] }
      }
    },
    "search_query": { "type": ["null", "string"] },
    "quantity": { "type": ["null", "integer"] },
    "total_price": { "type": ["null", "number"] },
    "transaction_id": { "type": ["null", "string"] },
    "payment_method": { "type": ["null", "string"] },
    "shipping_address": { "type": ["null", "string"] }
  }
}
"""


def load_products():
    """Load products from the JSON file."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        products_path = os.path.join(script_dir, PRODUCTS_FILE)
        
        with open(products_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            products = data.get('products', [])
            logger.info(f"Successfully loaded {len(products)} products from {PRODUCTS_FILE}")
            return products
    except FileNotFoundError:
        logger.error(f"Products file '{PRODUCTS_FILE}' not found. Using fallback product list...")
        return get_fallback_products()
    except Exception as e:
        logger.error(f"Error loading products: {e}. Using fallback product list...")
        return get_fallback_products()

def get_fallback_products():
    """Fallback product list in case JSON file is not available."""
    return [
        {'id': 101, 'name': 'Wireless Headphones', 'category': 'electronics', 'subcategory': 'audio', 'price': 249.99, 'brand': 'SonicWave'},
        {'id': 201, 'name': 'Slim Fit Jeans', 'category': 'fashion', 'subcategory': 'menswear', 'price': 59.99, 'brand': 'UrbanThreads'},
        {'id': 301, 'name': 'Coffee Maker', 'category': 'home', 'subcategory': 'kitchen', 'price': 89.99, 'brand': 'BrewMaster'},
    ]

products = load_products()


user_pool = []
for _ in range(USER_POOL_SIZE):
    user = {
        'user_id': fake.uuid4(),
        'demographics': {
            'age_group': random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
            'gender': random.choice(['M', 'F', 'O']),
            'country': fake.country_code()
        }
    }
    user_pool.append(user)


pages = ["/", "/home", "/products/electronics", "/products/fashion", "/products/home", "/products/books", "/deals"]
referrers = ["https://www.google.com", "https://www.bing.com", "https://www.youtube.com", "https://twitter.com", "https://www.facebook.com", "direct"]
devices = ['mobile', 'desktop', 'tablet']
browsers = ['chrome', 'safari', 'firefox', 'edge']


def generate_click_event():
    """Generates a detailed fake clickstream event."""
    user = random.choice(user_pool)
    product = random.choice(products)
    
    event_type = random.choices(
        ['page_view', 'product_click', 'add_to_cart', 'remove_from_cart', 'purchase', 'search'],
        weights=[0.60, 0.15, 0.10, 0.03, 0.05, 0.07],
        k=1
    )[0]

    event = {
        "user_id": user['user_id'],
        "session_id": fake.uuid4(),
        "demographics": user['demographics'],
        "event_type": event_type,
        "event_timestamp": int(time.time() * 1000),
        "device": random.choice(devices),
        "browser": random.choice(browsers),
        "os": random.choice(['windows', 'macos', 'linux', 'ios', 'android']),
        "page_url": random.choice(pages),
        "referrer_url": random.choice(referrers),
    }

    if event_type in ['product_click', 'add_to_cart', 'remove_from_cart', 'purchase']:
        event['product_details'] = product
        event['page_url'] = f"/product/{product['id']}"

    if event_type == 'search':
        event['search_query'] = fake.word() + " " + fake.word()

    if event_type in ['add_to_cart', 'purchase']:
        event['quantity'] = random.randint(1, 3)
        event['total_price'] = round(event['quantity'] * product['price'], 2)

    if event_type == 'purchase':
        event['transaction_id'] = fake.uuid4()
        event['payment_method'] = random.choice(['credit_card', 'paypal', 'apple_pay'])
        event['shipping_address'] = fake.address().replace('\n', ', ')

    return event


def delivery_callback(err, msg):
    """Delivery callback for Confluent Kafka producer."""
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    logger.info("Initializing Confluent Cloud producer with JSON Schema serializer...")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    json_serializer = JSONSerializer(CLICKSTREAM_SCHEMA, schema_registry_client)
    producer = Producer(CONFLUENT_CONFIG)

    logger.info(f"Starting to produce events to topic '{KAFKA_TOPIC}'. Press Ctrl+C to stop.")
    event_count = 0
    try:
        while True:
            event = generate_click_event()
            producer.produce(
                topic=KAFKA_TOPIC,
                key=event['user_id'],
                value=json_serializer(event, SerializationContext(KAFKA_TOPIC, MessageField.VALUE)),
                callback=delivery_callback
            )
            producer.poll(0)
            if event_count < 5 or event_count % 100 == 0:
                logger.info(f"Produced event #{event_count}: {event['event_type']} by user {event['user_id'][:8]}...")
            event_count += 1
            time.sleep(random.uniform(SEND_INTERVAL_MIN, SEND_INTERVAL_MAX))
    except KeyboardInterrupt:
        logger.info("\nUser interrupted the producer.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info(f"Flushing remaining messages. Total events produced: {event_count}")
        producer.flush()
        logger.info("Producer closed.")

if __name__ == '__main__':
    main()

