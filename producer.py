#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer
from config import config as configfile

if __name__ == '__main__':
    config = configfile['kafka']

    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    topic_2 = "TeamPython"
    team_domestic = [ "anhdt", "ducdm", "thuongpv", "loinv", "honglk"]
    team_export = ["thangnm", "khaid", "hoangnv"]
    team_python = [team_domestic, team_export]
    domestic, export = 0, 1
    team = [domestic, export]
    tasks = ["Edit order", "Add new item", "Delete order line", "Call ATP/CTP", "Search order"]

    for _ in range(15):
        
        team_got_selected = choice([0,1])
        coder = choice(team_python[team_got_selected])
        task = choice(tasks)
        producer.produce(topic_2, task, coder, partition = team_got_selected, callback=delivery_callback)
    producer.poll(10000)
    producer.flush()