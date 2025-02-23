import os
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger
from collections import deque

# Load Environment Variables
load_dotenv()

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("TEMPS_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_bootstrap_servers() -> str:
    """Fetch Kafka bootstrap servers from environment or use default."""
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Kafka bootstrap servers: {servers}")
    return servers

def create_kafka_consumer(topic: str) -> KafkaConsumer:
    """Create a Kafka consumer."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[get_bootstrap_servers()],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Kafka consumer created for topic: {topic}")
    return consumer

def main():
    """Main entry point for the consumer."""
    topic = get_kafka_topic()
    consumer = create_kafka_consumer(topic)

    logger.info(f"Starting to consume messages from topic '{topic}'...")

    # Initialize data storage and plot
    temperatures = deque(maxlen=100)
    timestamps = deque(maxlen=100)
    
    plt.ion()
    fig, ax = plt.subplots()
    line, = ax.plot(timestamps, temperatures, 'r-')
    
    # Configure x-axis for datetime
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    fig.tight_layout()

    # Add plot title and axis labels
    ax.set_title('Real-Time Temperature Data')
    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Temperature (°C)')

    try:
        for message in consumer:
            data = message.value
            logger.info(f"Consumed message: {data}")
            
            # Parse timestamp and update data storage
            timestamp = datetime.fromisoformat(data['timestamp'])
            timestamps.append(timestamp)
            temperatures.append(data['temperature'])
            
            # Update plot
            line.set_xdata(timestamps)
            line.set_ydata(temperatures)
            ax.relim()
            ax.autoscale_view()
            fig.canvas.draw()
            fig.canvas.flush_events()
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()
