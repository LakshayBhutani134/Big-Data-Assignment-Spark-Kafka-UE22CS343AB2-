# Big-Data-Assignment-Spark-Kafka-UE22CS343AB2-
This repository contains solutions to two tasks based on real-world big data processing using Apache Spark and Apache Kafka.

Task 1 – Olympic Data Analysis (Spark)
Task1.py: A PySpark script to analyze Olympic data from 2012, 2016, and 2020.

Identifies top-performing athletes by sport across three Olympic Games.

Recognizes top 5 international coaches from China, India, and the USA based on medal points.

Final output: a single .txt file listing top athletes and coaches in the required format.

Task 2 – Streaming Analytics Pipeline (Kafka)
kafka-producer.py: Kafka producer that streams raw event logs to different topics based on event type.

kafka-consumer1.py: Computes most used programming language and most difficult category to solve (Client 1).

kafka-consumer2.py: Generates competition leaderboards with calculated scores (Client 2).

kafka-consumer3.py: Calculates user contributions and ELO ratings based on submissions and upvotes (Client 3).

producer.py (optional): Alternate producer using JSON serialization for extended event types.
