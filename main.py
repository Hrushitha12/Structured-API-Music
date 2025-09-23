# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count as _count, avg as _avg,
    row_number, desc, asc, to_timestamp, hour, expr
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# -----------------------------------------
# Load datasets
# -----------------------------------------
logs = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv("listening_logs.csv")
    # normalize types
    .withColumn("duration_sec", col("duration_sec").cast("int"))
    .withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

songs = spark.read.option("header", True).option("inferSchema", True).csv("songs_metadata.csv")

# Join logs with metadata
j = logs.join(songs, on="song_id", how="left")

# -----------------------------------------
# Task 1: User Favorite Genres
# (by total listening duration; ties broken by play count, then genre name)
# -----------------------------------------
by_user_genre = (
    j.groupBy("user_id", "genre")
     .agg(
         _sum("duration_sec").alias("total_duration_sec"),
         _count("*").alias("plays")
     )
)

w = Window.partitionBy("user_id").orderBy(
    desc("total_duration_sec"),
    desc("plays"),
    asc("genre")
)

favorite_genre = (
    by_user_genre
    .withColumn("rn", row_number().over(w))
    .where(col("rn") == 1)
    .select(
        "user_id",
        col("genre").alias("favorite_genre"),
        "total_duration_sec",
        "plays"
    )
)

print("\n=== Task 1: Favorite Genre per User ===")
favorite_genre.show(20, truncate=False)


favorite_genre.write.mode("overwrite").parquet("out/favorite_genre_per_user")

# -----------------------------------------
# Task 2: Average Listen Time
# (a) per user, (b) per genre, plus global average
# -----------------------------------------
avg_listen_per_user = (
    logs.groupBy("user_id")
        .agg(
            _avg("duration_sec").alias("avg_duration_sec"),
            expr("percentile_approx(duration_sec, 0.5)").alias("median_duration_sec"),
            _count("*").alias("plays")
        )
        .orderBy("user_id")
)

avg_listen_per_genre = (
    j.groupBy("genre")
     .agg(
         _avg("duration_sec").alias("avg_duration_sec"),
         expr("percentile_approx(duration_sec, 0.5)").alias("median_duration_sec"),
         _count("*").alias("plays")
     )
     .orderBy("genre")
)

global_avg_listen = logs.select(
    _avg("duration_sec").alias("global_avg_duration_sec"),
    expr("percentile_approx(duration_sec, 0.5)").alias("global_median_duration_sec")
)

print("\n=== Task 2a: Average Listen Time per User ===")
avg_listen_per_user.show(20, truncate=False)
print("\n=== Task 2b: Average Listen Time per Genre ===")
avg_listen_per_genre.show(20, truncate=False)
print("\n=== Task 2c: Global Average Listen Time ===")
global_avg_listen.show(truncate=False)
avg_listen_per_user.write.mode("overwrite").parquet("out/avg_listen_per_user")
avg_listen_per_genre.write.mode("overwrite").parquet("out/avg_listen_per_genre")

# -----------------------------------------
# Task 3: Genre Loyalty Scores
# Loyalty = (duration in favorite genre) / (total duration by user)
# -----------------------------------------
total_by_user = (
    j.groupBy("user_id")
     .agg(_sum("duration_sec").alias("user_total_duration_sec"))
)

genre_loyalty = (
    favorite_genre.alias("fav")
    .join(total_by_user.alias("tot"), on="user_id")
    .withColumn(
        "genre_loyalty_score",
        (col("fav.total_duration_sec") / col("tot.user_total_duration_sec")).cast("double")
    )
    .select(
        col("user_id"),
        col("fav.favorite_genre").alias("favorite_genre"),
        col("tot.user_total_duration_sec"),
        col("fav.total_duration_sec").alias("favorite_genre_duration_sec"),
        col("genre_loyalty_score")
    )
    .orderBy(desc("genre_loyalty_score"))
)

print("\n=== Task 3: Genre Loyalty Scores (per User) ===")
genre_loyalty.show(20, truncate=False)
genre_loyalty.write.mode("overwrite").parquet("out/genre_loyalty_scores")

# -----------------------------------------
# Task 4: Identify users who listen between 12 AM and 5 AM
# Define night window as hours 0,1,2,3,4 (i.e., 00:00–04:59:59)
# -----------------------------------------
night_logs = logs.withColumn("hour", hour(col("ts"))).where(col("hour").between(0, 4))

night_users = (
    night_logs.groupBy("user_id")
              .agg(
                  _count("*").alias("night_plays"),
                  _sum("duration_sec").alias("night_duration_sec")
              )
              .orderBy(desc("night_plays"), desc("night_duration_sec"))
)

print("\n=== Task 4: Night-owl Users (12 AM–5 AM) ===")
night_users.show(20, truncate=False)
night_users.write.mode("overwrite").parquet("out/night_owl_users")

# Optional: stop the session if running as a standalone script
# spark.stop()
