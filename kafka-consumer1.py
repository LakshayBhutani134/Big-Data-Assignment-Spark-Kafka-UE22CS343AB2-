#!/usr/bin/env python3

import sys
import json
from kafka import KafkaConsumer

class KafkaMessageConsumer:
    def __init__(self, problem_topic, bootstrap_servers='localhost:9092'):
        self.problem_topic = problem_topic
        self.consumer = KafkaConsumer(problem_topic,bootstrap_servers=bootstrap_servers,)
        self.language_count = {}
        self.category_pass_fail = {}

    def process_message(self, message):
        line = message.value.decode('utf-8').strip()
        if line == "EOF":
            return False

        data = line.split()
        if data[0] == "problem":
            _, user_id, problem_id, category, difficulty, submission_id, status, language, runtime = data
            self.track_language(language)
            self.track_category_status(category, status)
        return True

    def get_most_used_languages(self):
        if not self.language_count:
            return []

        max_lang_usage = max(self.language_count.values())
        return sorted([lang for lang, count in self.language_count.items() if count == max_lang_usage])

    def track_language(self, language):
        if language not in self.language_count:
            self.language_count[language] = 0
        self.language_count[language] += 1

    def track_category_status(self, category, status):
        if category not in self.category_pass_fail:
            self.category_pass_fail[category] = [0, 0] 

        if status == "Passed":
            self.category_pass_fail[category][0] += 1  
        self.category_pass_fail[category][1] += 1  


    def generate_report(self):
        result = {
            "most_used_language": self.get_most_used_languages(),
            "most_difficult_category": self.get_most_difficult_categories()
        }
        return json.dumps(result, indent=4)
        
    def close(self):
        self.consumer.close()
        
    def get_most_difficult_categories(self):
        if not self.category_pass_fail:
            return []
        min_pass_ratio = min((pf[0] / pf[1]) for pf in self.category_pass_fail.values() if pf[1] > 0)
        return sorted([cat for cat, pf in self.category_pass_fail.items() if pf[1] > 0 and (pf[0] / pf[1]) == min_pass_ratio])

def main():
    problem_topic = sys.argv[1]
    consumer = KafkaMessageConsumer(problem_topic)

    try:
        for message in consumer.consumer:
            if not consumer.process_message(message):
                break
        output_c1 = consumer.generate_report()
        print(output_c1)

    finally:
        consumer.close()

if __name__ == "__main__":
    main()

