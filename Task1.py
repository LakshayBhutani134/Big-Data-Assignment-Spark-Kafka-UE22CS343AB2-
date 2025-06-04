#!/usr/bin/env python3

import os, sys
sys.executable
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

spark = SparkSession.builder \
    .appName("OlympicDataProcessing") \
    .getOrCreate()

def uppercase_string_columns(df):
    return df.select([F.upper(F.col(c)).alias(c) if df.schema[c].dataType == T.StringType() else c for c in df.columns])

def load_csv_data(file_name):
    return spark.read.csv(file_name, header=True, inferSchema=True)

athlete_2012_data = load_csv_data(sys.argv[1])
athlete_2016_data = load_csv_data(sys.argv[2])
athlete_2020_data = load_csv_data(sys.argv[3])
coach_data = load_csv_data(sys.argv[4])
medal_data = load_csv_data(sys.argv[5])

athlete_2012_data = uppercase_string_columns(athlete_2012_data)
athlete_2016_data = uppercase_string_columns(athlete_2016_data)
athlete_2020_data = uppercase_string_columns(athlete_2020_data)
medal_data = uppercase_string_columns(medal_data)
coach_data = uppercase_string_columns(coach_data)

points_2012 = {"Gold": 20, "Silver": 15, "Bronze": 10}
points_2016 = {"Gold": 12, "Silver": 8, "Bronze": 6}
points_2020 = {"Gold": 15, "Silver": 12, "Bronze": 7}

def assign_medal_points(df):
    return df.withColumn(
        "points",
        F.when((F.col("year") == 2012) & (F.col("medal") == "GOLD"), points_2012["Gold"])
         .when((F.col("year") == 2012) & (F.col("medal") == "SILVER"), points_2012["Silver"])
         .when((F.col("year") == 2012) & (F.col("medal") == "BRONZE"), points_2012["Bronze"])
         .when((F.col("year") == 2016) & (F.col("medal") == "GOLD"), points_2016["Gold"])
         .when((F.col("year") == 2016) & (F.col("medal") == "SILVER"), points_2016["Silver"])
         .when((F.col("year") == 2016) & (F.col("medal") == "BRONZE"), points_2016["Bronze"])
         .when((F.col("year") == 2020) & (F.col("medal") == "GOLD"), points_2020["Gold"])
         .when((F.col("year") == 2020) & (F.col("medal") == "SILVER"), points_2020["Silver"])
         .when((F.col("year") == 2020) & (F.col("medal") == "BRONZE"), points_2020["Bronze"])
    )

medal_data = assign_medal_points(medal_data)

def add_year_column(df, year):
    return df.withColumn("year", F.lit(year))

athlete_2012_with_year = add_year_column(athlete_2012_data, 2012)
athlete_2016_with_year = add_year_column(athlete_2016_data, 2016)
athlete_2020_with_year = add_year_column(athlete_2020_data, 2020)

combined_athlete_data = athlete_2012_with_year.union(athlete_2016_with_year).union(athlete_2020_with_year)

def calculate_athlete_points(medal_df, athlete_df):
    return medal_df.join(athlete_df, on=["id", "sport", "year", "event"], how="inner") \
        .groupBy("id", "name", "sport") \
        .agg(
            F.sum("points").alias("total_points"),
            F.sum(F.when(F.col("medal") == "GOLD", 1).otherwise(0)).alias("gold_count"),
            F.sum(F.when(F.col("medal") == "SILVER", 1).otherwise(0)).alias("silver_count"),
            F.sum(F.when(F.col("medal") == "BRONZE", 1).otherwise(0)).alias("bronze_count")
        )

athlete_point_totals = calculate_athlete_points(medal_data, combined_athlete_data)

athlete_window = Window.partitionBy("sport").orderBy(
    F.desc("total_points"),
    F.desc("gold_count"),
    F.desc("silver_count"),
    F.desc("bronze_count"),
    F.asc("name")
)

top_athletes_per_sport = athlete_point_totals.withColumn("rank", F.row_number().over(athlete_window)) \
    .filter(F.col("rank") == 1) \
    .select("name", "sport") \
    .orderBy("sport")

top_athlete_names = [row['name'] for row in top_athletes_per_sport.collect()]

def clean_athlete_medal_data(athlete_df, medal_df):
    return athlete_df.join(
        medal_df,
        (athlete_df["ID"] == medal_df["ID"]) &
        (athlete_df["SPORT"] == medal_df["SPORT"]) &
        (athlete_df["EVENT"] == medal_df["EVENT"]) &
        (athlete_df["YEAR"] == medal_df["YEAR"]),
        how="inner"
    ).drop(
        "DOB", "HEIGHT(m)", "WEIGHT(kg)", "NUM_FOLLOWERS", "NUM_ARTICLES", "PERSONAL_BEST",
        "EVENT", athlete_df["ID"], athlete_df["sport"], medal_df["country"], athlete_df["year"]
    )

cleaned_athlete_medal_data = clean_athlete_medal_data(combined_athlete_data, medal_data)


def join_coach_data(cleaned_df, coach_df):
    return cleaned_df.join(
        coach_df,
        (cleaned_df["COACH_ID"] == coach_df["ID"]) &
        (cleaned_df["SPORT"] == coach_df["SPORT"]),
        how="inner"
    )

def get_points(year_column, medal_column):
    return (F.when(year_column == 2012,
            F.when(medal_column == "GOLD", 20)
             .when(medal_column == "SILVER", 15)
             .when(medal_column == "BRONZE", 10).otherwise(0))
        .when(year_column == 2016,
            F.when(medal_column == "GOLD", 12)
             .when(medal_column == "SILVER", 8)
             .when(medal_column == "BRONZE", 6).otherwise(0))
        .when(year_column == 2020,
            F.when(medal_column == "GOLD", 15)
             .when(medal_column == "SILVER", 12)
             .when(medal_column == "BRONZE", 7).otherwise(0))
        .otherwise(0))
    
athlete_coach_data = join_coach_data(cleaned_athlete_medal_data, coach_data)

def further_clean_data(df, athlete_df, coach_df):
    return df.drop(
        athlete_df["NAME"], "YEAR", coach_df["ID"], coach_df["age"], coach_df["years_of_experience"],
        coach_df["country"], coach_df["contract_id"], coach_df["certification_committee"]
    )

cleaned_data_v2 = further_clean_data(athlete_coach_data, combined_athlete_data, coach_data)

coach_window = Window.partitionBy("COUNTRY").orderBy(
    F.desc("TOTAL_POINTS"),
    F.desc("GOLD_COUNT"),
    F.desc("SILVER_COUNT"),
    F.desc("BRONZE_COUNT"),
    F.asc("NAME")
)

cleaned_data_v2[cleaned_data_v2['COACH_ID'] =='C_000014']

coach_ranking = cleaned_data_v2.groupBy("COUNTRY", "COACH_ID", "NAME") \
    .agg(
        F.sum("POINTS").alias("TOTAL_POINTS"),
        F.sum(F.when(F.col("MEDAL") == "GOLD", 1).otherwise(0)).alias("GOLD_COUNT"),
        F.sum(F.when(F.col("MEDAL") == "SILVER", 1).otherwise(0)).alias("SILVER_COUNT"),
        F.sum(F.when(F.col("MEDAL") == "BRONZE", 1).otherwise(0)).alias("BRONZE_COUNT")
    ).withColumn("RANK", F.row_number().over(coach_window))

def filter_top_coaches(df, countries, rank_limit):
    return df.filter((df["COUNTRY"].isin(countries)) & (df["RANK"] <= rank_limit))

target_countries = ["INDIA", "USA", "CHINA"]
top_coaches = filter_top_coaches(coach_ranking, target_countries, 5)

def prepare_output(top_athletes, top_coaches):
    athletes_str = '", "'.join(top_athletes)
    coaches_str = '", "'.join(top_coaches)
    return f'(["{athletes_str}"], ["{coaches_str}"])'

top_athletes_list = top_athlete_names
top_coaches_list = [row['NAME'] for row in top_coaches.select("NAME").collect()]

output = prepare_output(top_athletes_list, top_coaches_list)

with open(sys.argv[6] , 'w') as f:
	f.write(str(output))

spark.stop()

