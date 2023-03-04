import pyspark
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

def input_paths(date, depth):

# Вычитаем дни из даты
    end = datetime.strptime(date, "%Y-%m-%d")
    start = end - timedelta(days=depth-1)
    
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]
    
    paths = list()
    
    for date in date_generated:
        tag_date = date.strftime("%Y-%m-%d")
        paths.append(f"/user/nickperegr/data/events/date={tag_date}/event_type=message")
    
    return paths

spark = SparkSession \
        .builder \
        .master("yarn")\
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", 2) \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores",2) \
        .appName("test") \
        .getOrCreate()


paths = input_paths('2022-05-31', 7)

df = spark.read.parquet(*paths)

#если в схеме есть массив, то нужно использовать explode
df_comp = df.filter( F.col('event.message_channel_to').isNotNull())\
            .selectExpr('event.message_from as user', 'explode(event.tags) as tag')\
            .groupBy('tag')\
            .agg(F.countDistinct('user').alias('suggested_count'))\
            .where('suggested_count >= 100')

df_tags_verified = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")

df_joined = df_comp.join(df_tags_verified, ["tag"], how = "left_anti")

df_joined.write\
    .mode("overwrite")\
    .format("parquet")\
    .save(f"/user/username/data/analytics/candidates_d{depth}_pyspark")