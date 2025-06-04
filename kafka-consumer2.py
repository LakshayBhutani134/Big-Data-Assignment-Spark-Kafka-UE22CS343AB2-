#!/usr/bin/env python3

import sys
import json
from kafka import KafkaConsumer

class CompetitionLeaderboard:
    def __init__(self, competition_topic, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(competition_topic,bootstrap_servers=bootstrap_servers)
        self.leaderboard = {}

    def print_leaderboard(self):
        sorted_leaderboard = {comp_id: dict(sorted(users.items())) for comp_id, users in sorted(self.leaderboard.items())}
        print(json.dumps(sorted_leaderboard, indent=4))
        
    def process_message(self, message):
        line = message.value.decode('utf-8').strip()

        if line == "EOF":
            self.print_leaderboard()
            return False

        try:
            data = line.split()
            if data[0] == "competition":
                comp_id = data[1]
                user_id = data[2]
                com_problem_id = data[3]
                category = data[4]
                difficulty = data[5]
                comp_submission_id = data[6]
                status = data[7]
                language = data[8]
                runtime = int(data[9])
                time_taken = int(data[10])

                points = self.calculate_points(status, difficulty, runtime, time_taken)

                self.update_leaderboard(comp_id, user_id, points)

        except Exception as e:
            print(f"Error in message: {line}, Error: {e}")

        return True

    def close(self):
        self.consumer.close()

    def update_leaderboard(self, comp_id, user_id, points):
        if comp_id not in self.leaderboard:
            self.leaderboard[comp_id] = {}
        if user_id not in self.leaderboard[comp_id]:
            self.leaderboard[comp_id][user_id] = 0

        self.leaderboard[comp_id][user_id] += points

    def calculate_points(self, status, difficulty, runtime, time_taken):
        status_score = 100 if status == "Passed" else (20 if status == "TLE" else 0)

        difficulty_score = {"Hard": 3,"Easy": 1,"Medium": 2, }.get(difficulty, 0)

        runtime_bonus = 10000 / runtime if runtime > 0 else 0
        time_taken_penalty = 0.25 * time_taken
        bonus = max(1, (1 + runtime_bonus - time_taken_penalty))

        return int(status_score * difficulty_score * bonus)

def main():
    competition_topic = sys.argv[2]

    leaderboard = CompetitionLeaderboard(competition_topic)

    try:
        for message in leaderboard.consumer:
            if not leaderboard.process_message(message):
                break
    finally:
        leaderboard.close()

if __name__ == "__main__":
    main()

