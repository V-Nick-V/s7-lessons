#/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster realization.py 2022-06-04 5 /user/nickperegr/data/events /user/nickperegr/data/tmp/tag_tops_06_04_5
#/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster realization.py 2022-05-04 5 /user/nickperegr/data/events /user/nickperegr/data/tmp/tag_tops_05_04_5
#/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster realization.py 2022-05-04 1 /user/nickperegr/data/events /user/nickperegr/data/tmp/tag_tops_05_04_1


import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
from pyspark.sql.types import *

def input_paths(date, depth, base_input_path):

# Вычитаем дни из даты
    end = datetime.strptime(date, "%Y-%m-%d")
    start = end - timedelta(days=depth-1)
    
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]
    
    paths = list()
    
    for date in date_generated:
        tag_date = date.strftime("%Y-%m-%d")
        paths.append(f"{base_input_path}/date={tag_date}/event_type=message")
    
    return paths

def main():

    date = sys.argv[1]
    depth = sys.argv[2]
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]

    spark = SparkSession \
        .builder \
        .master("yarn")\
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", 2) \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores",2) \
        .appName("tag_tops") \
        .getOrCreate()

    paths = input_paths(f"{date}", int(depth), f"{base_input_path}") 

result = spark.read \
        .option("basePath", "/user/nickperegr/data/events") \
        .parquet(*paths) \
        .where("event_type = 'message'") \
        .where("event.message_channel_to is not null") \
        .select(F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").alias("user_id"),
                F.explode(F.col("event.tags")).alias("tag")) \
        .groupBy("user_id", "tag") \
        .agg(F.count("*").alias("tag_count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id") \
                                                .orderBy(F.desc("tag"), F.desc("tag_count")))) \
        .where("rank <= 3") \
        .groupBy("user_id") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag")) \
        .withColumnRenamed("1", "tag_top_1") \
        .withColumnRenamed("2", "tag_top_2") \
        .withColumnRenamed("3", "tag_top_3")

result.write\
    .mode("overwrite")\
    .format("parquet")\
    .save(f"{base_output_path}")

if __name__ == "__main__":
        main()