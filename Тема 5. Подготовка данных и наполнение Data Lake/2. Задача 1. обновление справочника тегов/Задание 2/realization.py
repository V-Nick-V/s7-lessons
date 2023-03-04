import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"/user/nickperegr/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

paths = input_paths('2022-05-31', 7)

spark = SparkSession \
        .builder \
        .master("yarn")\
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", 2) \
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores",2) \
        .appName("test") \
        .getOrCreate()

messages = spark.read.parquet(*paths)

messages = spark.read.parquet(*paths)
all_tags = messages.where("event.message_channel_to is not null").selectExpr(["event.message_from as user", "explode(event.tags) as tag"]).groupBy("tag").agg(F.expr("count(distinct user) as suggested_count")).where("suggested_count >= 100")
verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
candidates = all_tags.join(verified_tags, "tag", "left_anti")

candidates.write.mode("overwrite").parquet('/user/nickperegr/data/analytics/candidates_d7_pyspark')