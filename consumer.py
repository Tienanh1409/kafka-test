#!/usr/bin/env python
from confluent_kafka import Consumer, OFFSET_BEGINNING
from config import config as configfile

if __name__ == '__main__':
     config = configfile['kafka']
     config.update({"group.id": "python_example_group_1"})

     # Create Consumer instance
     consumer = Consumer(config)

     # Set up a callback to handle the '--reset' flag.
     def reset_offset(consumer, partitions):
          for p in partitions:
               p.offset = OFFSET_BEGINNING
          consumer.assign(partitions)

     # Subscribe to topic
     topic = "TeamPython"
     consumer.subscribe([topic], on_assign=reset_offset)
     consumer.assign()

     # Poll for new messages from Kafka and print them.
     try:
          while True:
               msg = consumer.poll(1.0)
               if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
               elif msg.error():
                    print("ERROR: %s".format(msg.error()))
               else:
                    # Extract the (optional) key and value, and print.

                    print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
     except KeyboardInterrupt:
          pass
     finally:
          consumer.close()