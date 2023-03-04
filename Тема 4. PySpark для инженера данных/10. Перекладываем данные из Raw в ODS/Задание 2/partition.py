import sys
from datetime import date as dt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def main():

    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    print(f'Date is {date}')
    print(f'Your source directory is {base_input_path}')
    print(f'Your target directory is {base_output_path}')

    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)    
    	
    events = sql.read.json(f"{base_input_path}/date={date}/")

    events.write\
        .partitionBy("event_type")\
        .mode("overwrite")\
        .format("parquet")\
		.save(f"{base_output_path }/date={date}/")

if __name__ == "__main__":
        main()