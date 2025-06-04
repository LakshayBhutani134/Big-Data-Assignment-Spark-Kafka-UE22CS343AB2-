#!/usr/bin/env python3

import sys
from kafka import KafkaProducer

class KafkaMessageProducer:
    def __init__(self, problem_topic, competition_topic, solution_topic, bootstrap_servers='localhost:9092'):
        self.problem_topic = problem_topic
        self.competition_topic = competition_topic
        self.solution_topic = solution_topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def close(self):
        self.producer.close()   

    def send_message(self, topic, message):
        self.producer.send(topic, value=message.encode('utf-8'))
        
    def process_message(self, message):
        data = message.split()
        
        if data[0] == "solution":
            self.send_message(self.solution_topic, message)
        elif data[0] == "competition":
            self.send_message(self.competition_topic, message)
        elif data[0] == "problem":
            self.send_message(self.problem_topic, message)
        else:
            self.send_message(self.problem_topic, message)
            self.send_message(self.solution_topic, message)
            self.send_message(self.competition_topic, message)

def main():
    problem_topic = sys.argv[1]
    competition_topic = sys.argv[2]
    solution_topic = sys.argv[3]

    producer = KafkaMessageProducer(problem_topic, competition_topic, solution_topic)

    try:
        for line in sys.stdin:
            message = line.strip()
            if message:
                producer.process_message(message)
    finally:
        producer.close()

if __name__ == "__main__":
    main()

