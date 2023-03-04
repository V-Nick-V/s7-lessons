#/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster verified_tags_candidates.py 2022-05-31 7 300 /user/nickperegr/data/events /user/master/data/snapshots/tags_verified/actual /user/nickperegr/data/analytics/verified_tags_candidates_d7

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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
    threshold = sys.argv[3]
    base_input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    base_output_path = sys.argv[6]

    spark = SparkSession \
        .builder \
        .master("yarn")\
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", 2) \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores",2) \
        .appName(f"VerifiedTagsCandidatesJob-[{date}]-d[{depth}]-cut[{threshold}]") \
        .getOrCreate()

    paths = input_paths(f"{date}", int(depth), f"{base_input_path}") 

    df = spark.read.parquet(*paths)

    #если в схеме есть массив, то нужно использовать explode
    df_comp = df.filter( F.col('event.message_channel_to').isNotNull())\
            .selectExpr('event.message_from as user', 'explode(event.tags) as tag')\
            .groupBy('tag')\
            .agg(F.countDistinct('user').alias('suggested_count'))\
            .where(f'suggested_count >= {threshold}')

    df_tags_verified = spark.read.parquet(f"{verified_tags_path}")

    df_joined = df_comp.join(F.broadcast(df_tags_verified), ["tag"], how = "left_anti")

    df_joined.write\
    .mode("overwrite")\
    .format("parquet")\
    .save(f"{base_output_path}")

if __name__ == "__main__":
        main()