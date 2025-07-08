import json
import time
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import logging
import threading

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventConsumer:
    def __init__(self, bootstrap_servers='localhost:9093', topic='user-events', 
                 group_id='analytics-group', consumer_id='consumer-1'):
        self.topic = topic
        self.group_id = group_id
        self.consumer_id = consumer_id
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=consumer_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        # Analytics storage
        self.user_actions = defaultdict(list)
        self.action_counts = Counter()
        self.total_revenue = 0.0
        self.hourly_stats = defaultdict(lambda: defaultdict(int))
        self.user_sessions = defaultdict(set)
        
        # Statistics display thread
        self.stats_lock = threading.Lock()
        self.running = True
        
    def process_event(self, event):
        """Process individual event and update analytics"""
        with self.stats_lock:
            # Basic event counting
            self.action_counts[event['action']] += 1
            
            # User activity tracking
            user_id = event['user_id']
            self.user_actions[user_id].append({
                'action': event['action'],
                'timestamp': event['timestamp'],
                'product': event.get('product'),
                'amount': event.get('amount')
            })
            
            # Session tracking
            if event.get('session_id'):
                self.user_sessions[user_id].add(event['session_id'])
            
            # Revenue calculation
            if event['action'] == 'purchase' and event.get('amount'):
                self.total_revenue += event['amount']
            
            # Hourly statistics
            timestamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            hour_key = timestamp.strftime('%Y-%m-%d %H:00')
            self.hourly_stats[hour_key][event['action']] += 1
    
    def calculate_user_analytics(self, user_id):
        """Calculate specific analytics for a user"""
        user_events = self.user_actions[user_id]
        if not user_events:
            return {}
        
        # Count actions per user
        user_action_counts = Counter(event['action'] for event in user_events)
        
        # Calculate user revenue
        user_revenue = sum(
            event['amount'] for event in user_events 
            if event['action'] == 'purchase' and event['amount']
        )
        
        # Session count
        session_count = len(self.user_sessions[user_id])
        
        # Most viewed products
        viewed_products = [
            event['product'] for event in user_events 
            if event['action'] == 'view_product' and event['product']
        ]
        
        return {
            'user_id': user_id,
            'total_actions': len(user_events),
            'action_breakdown': dict(user_action_counts),
            'total_revenue': user_revenue,
            'session_count': session_count,
            'most_viewed_products': Counter(viewed_products).most_common(3)
        }
    
    def get_top_users_by_activity(self, limit=5):
        """Get top users by activity"""
        user_activity = [
            (user_id, len(actions)) 
            for user_id, actions in self.user_actions.items()
        ]
        return sorted(user_activity, key=lambda x: x[1], reverse=True)[:limit]
    
    def get_conversion_rate(self):
        """Calculate conversion rate (purchases / total actions)"""
        total_actions = sum(self.action_counts.values())
        purchases = self.action_counts.get('purchase', 0)
        return (purchases / total_actions * 100) if total_actions > 0 else 0
    
    def display_analytics(self):
        """Display current analytics"""
        with self.stats_lock:
            print("\n" + "="*60)
            print(f"📊 KAFKA CONSUMER ANALYTICS - {self.consumer_id}")
            print(f"👥 Consumer Group: {self.group_id}")
            print("="*60)
            
            # Basic statistics
            print(f"📈 Total Events Processed: {sum(self.action_counts.values())}")
            print(f"💰 Total Revenue: ${self.total_revenue:.2f}")
            print(f"📊 Conversion Rate: {self.get_conversion_rate():.2f}%")
            
            # Action breakdown
            print("\n🔄 Action Breakdown:")
            for action, count in self.action_counts.most_common():
                percentage = (count / sum(self.action_counts.values()) * 100) if sum(self.action_counts.values()) > 0 else 0
                print(f"  • {action}: {count} ({percentage:.1f}%)")
            
            # Top active users
            top_users = self.get_top_users_by_activity()
            if top_users:
                print("\n👤 Top Active Users:")
                for i, (user_id, activity_count) in enumerate(top_users, 1):
                    user_analytics = self.calculate_user_analytics(user_id)
                    print(f"  {i}. {user_id}: {activity_count} actions, ${user_analytics['total_revenue']:.2f} revenue")
            
            # Recent hourly activity
            if self.hourly_stats:
                print("\n🕐 Recent Hourly Activity:")
                recent_hours = sorted(self.hourly_stats.keys())[-3:]  # Last 3 hours
                for hour in recent_hours:
                    total_hour_actions = sum(self.hourly_stats[hour].values())
                    print(f"  • {hour}: {total_hour_actions} actions")
            
            print("="*60)
    
    def stats_display_thread(self):
        """Thread function to display stats periodically"""
        while self.running:
            time.sleep(10)  # Display stats every 10 seconds
            if sum(self.action_counts.values()) > 0:
                self.display_analytics()
    
    def start_consuming(self):
        """Start consuming messages"""
        logger.info(f"Starting consumer: {self.consumer_id}")
        logger.info(f"Consumer Group: {self.group_id}")
        logger.info(f"Topic: {self.topic}")
        logger.info("Press Ctrl+C to stop")
        
        # Start stats display thread
        stats_thread = threading.Thread(target=self.stats_display_thread)
        stats_thread.daemon = True
        stats_thread.start()
        
        try:
            for message in self.consumer:
                logger.info(f"Received message from partition {message.partition}, offset {message.offset}")
                logger.info(f"Key: {message.key}, Value: {message.value}")
                
                # Process the event
                self.process_event(message.value)
                
                # Log processing
                logger.info(f"✅ Event processed: {message.value['action']} by {message.value['user_id']}")
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")
        
        # Final analytics display
        print("\n🔚 FINAL ANALYTICS REPORT")
        self.display_analytics()

if __name__ == "__main__":
    import sys
    
    # Allow specifying consumer ID as command line argument
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else 'consumer-1'
    
    # Create and start consumer
    consumer = EventConsumer(consumer_id=consumer_id)
    consumer.start_consuming()