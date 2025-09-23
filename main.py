# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets


# Task 1: User Favorite Genres


# Task 2: Average Listen Time



# Task 3: Genre Loyalty Scores


# Task 4: Identify users who listen between 12 AM and 5 AM
