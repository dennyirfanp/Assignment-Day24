import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, bootstrap_servers='localhost:9093', topic='user-events'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        
        # Create topic with specific partitions and replication
        self.create_topic_if_not_exists()
    
    def create_topic_if_not_exists(self):
        """Create topic with specific partition and replication settings"""
        from kafka.admin import KafkaAdminClient, NewTopic
        
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9093',
            client_id='producer_admin'
        )
        
        try:
            # Create topic with 3 partitions and replication factor 1
            topic_list = [
                NewTopic(
                    name=self.topic,
                    num_partitions=3,
                    replication_factor=1
                )
            ]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic '{self.topic}' created with 3 partitions and replication factor 1")
        except Exception as e:
            logger.info(f"Topic '{self.topic}' already exists or creation failed: {e}")
        finally:
            admin_client.close()
    
    def generate_random_event(self):
        """Generate random user event data"""
        users = ['user001', 'user002', 'user003', 'user004', 'user005']
        actions = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'search']
        products = ['laptop', 'phone', 'tablet', 'headphones', 'keyboard', 'mouse']
        
        event = {
            'event_id': f"evt_{int(time.time())}_{random.randint(1000, 9999)}",
            'user_id': random.choice(users),
            'action': random.choice(actions),
            'timestamp': datetime.now().isoformat(),
            'product': random.choice(products) if random.choice(actions) in ['purchase', 'view_product', 'add_to_cart'] else None,
            'amount': round(random.uniform(10.0, 1000.0), 2) if random.choice(actions) == 'purchase' else None,
            'session_id': f"sess_{random.randint(100000, 999999)}",
            'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            'user_agent': random.choice(['Chrome/91.0', 'Firefox/89.0', 'Safari/14.0', 'Edge/91.0'])
        }
        
        return event
    
    def send_event(self):
        """Send a single event to Kafka"""
        event = self.generate_random_event()
        
        # Use user_id as key for partitioning
        key = event['user_id']
        
        try:
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Event sent successfully:")
            logger.info(f"  Topic: {record_metadata.topic}")
            logger.info(f"  Partition: {record_metadata.partition}")
            logger.info(f"  Offset: {record_metadata.offset}")
            logger.info(f"  Event: {event}")
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
    
    def start_producing(self, interval=5):
        """Start producing events every 'interval' seconds"""
        logger.info(f"Starting producer. Sending events every {interval} seconds...")
        logger.info(f"Topic: {self.topic}")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                self.send_event()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    # Create and start producer
    producer = EventProducer()
    producer.start_producing(interval=5)