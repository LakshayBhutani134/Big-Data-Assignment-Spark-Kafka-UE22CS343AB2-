#!/usr/bin/env python3

import sys
import json
from kafka import KafkaConsumer

class ScoreCalculator:
    def __init__(self, starting_rating=1200):
        self.user_ratings = {}
        self.user_upvotes = {}
        self.default_rating = starting_rating
        
    def finalize_ratings(self):
        for user in self.user_ratings:
            self.user_ratings[user] = int(self.user_ratings[user])
            
    def sort_ratings(self):
        self.user_ratings = dict(sorted(self.user_ratings.items()))
        
    def compute_score(self, verdict, complexity, time_taken):
        score_map = {"Failed": -0.3, "Passed": 1, "TLE": 0.2,}
        difficulty_map = {"Hard": 1, "Easy": 0.3, "Medium": 0.7}
        status_score = score_map.get(verdict, 0)
        difficulty_score = difficulty_map.get(complexity, 0)
        time_bonus = 10000 / time_taken 
        K_factor = 32
        return K_factor * (status_score * difficulty_score) + time_bonus
        
    def get_top_contributor(self):
        if self.user_upvotes:
            top_users = [user for user, count in self.user_upvotes.items() if count == max(self.user_upvotes.values())]
            top_users.sort()
            return top_users
        return []
        
    def update_user_rating(self, user, points):
        if user not in self.user_ratings:
            self.user_ratings[user] = self.default_rating
        self.user_ratings[user] += points

    def increment_upvotes(self, user, votes):
        if user not in self.user_upvotes:
            self.user_upvotes[user] = 0
        self.user_upvotes[user] += votes

def handle_message(fields, calculator):
    event_type = fields[0]

    if event_type == "problem":
        user = fields[1]
        verdict = fields[6]
        complexity = fields[4]
        try:
            time_taken = float(fields[8])
        except ValueError:
            return
        score = calculator.compute_score(verdict, complexity, time_taken)
        calculator.update_user_rating(user, score)

    elif event_type == "competition":
        user = fields[2]
        verdict = fields[7]
        complexity = fields[5]
        try:
            time_taken = float(fields[9])
        except ValueError:
            return
        score = calculator.compute_score(verdict, complexity, time_taken)
        calculator.update_user_rating(user, score)

    elif event_type == "solution":
        user = fields[1]
        try:
            vote_count = int(fields[4])
        except ValueError:
            return
        calculator.increment_upvotes(user, vote_count)

def main():
    EOF_counter = 0
    consumer = KafkaConsumer(sys.argv[1],sys.argv[2],sys.argv[3], bootstrap_servers='localhost:9092',value_deserializer=lambda x: x.decode('utf-8'))
    score_calculator = ScoreCalculator()
    for message in consumer:
        fields = message.value.split()
        if fields[0] == "EOF":
            EOF_counter += 1
            if EOF_counter == 3:
                break
        handle_message(fields, score_calculator)

    score_calculator.finalize_ratings()
    score_calculator.sort_ratings()

    output = {
        "best_contributor": score_calculator.get_top_contributor(),
        "user_elo_rating": score_calculator.user_ratings
    }
    print(json.dumps(output, indent=4))

if __name__ == "__main__":
    main()

