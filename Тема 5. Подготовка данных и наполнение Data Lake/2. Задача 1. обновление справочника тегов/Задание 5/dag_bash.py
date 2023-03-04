import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                'owner': 'airflow',
                'start_date':pendulum.datetime(2020, 1, 1),
               }

dag_spark = DAG(
                dag_id = "datalake_etl",
                default_args=default_args,
                schedule_interval=None,
               )

base_config = {
    "task_id":"spark_submit_task",
    "conn_id":"yarn_spark",
    "application": "/lessons/partition1.py",
    "executor_cores":2,
    "executor_memory":"2g",
    "application_args":["2022-05-31","/user/master/data/events","/user/nickperegr/data/events"],
    }

d7_config = {
    "task_id":"spark_submit_task_d7",
    "conn_id":"yarn_spark",
    "application": "/lessons/verified_tags_candidates.py",
    "executor_cores":2,
    "executor_memory":"2g",
    "application_args":["2022-05-31", "7", "300", "/user/nickperegr/data/events","/user/master/data/snapshots/tags_verified/actual", "/user/nickperegr/data/analytics/verified_tags_candidates_d7"],
    }

d84_config = {
    "task_id":"spark_submit_task_d84",
    "conn_id":"yarn_spark",
    "application": "/lessons/verified_tags_candidates.py",
    "executor_cores":2,
    "executor_memory":"2g",
    "application_args":["2022-05-31", "84", "300", "/user/nickperegr/data/events","/user/master/data/snapshots/tags_verified/actual", "/user/nickperegr/data/analytics/verified_tags_candidates_d84"],
    }

spark_base_config = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "cluster",
    "spark.driver.maxResultSize": "20G",
    "spark.yarn.driver.memoryOverhead":"1024",
    "spark.yarn.executor.memoryOverhead":"1024",
    "spark.network.timeout":"15000s",
    "spark.executor.heartbeatInterval":"1500s",
    "spark.task.maxDirectResultSize":"8G",
    "spark.ui.view.acls":"*"
} 

spark_config = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "cluster",
    "spark.sql.execution.arrow.enabled":"true",
    "spark.driver.maxResultSize": "20G",
    "spark.yarn.driver.memoryOverhead":"1024",
    "spark.yarn.executor.memoryOverhead":"1024",
    "spark.network.timeout":"15000s",
    "spark.executor.heartbeatInterval":"1500s",
    "spark.task.maxDirectResultSize":"8G",
    "spark.ui.view.acls":"*"
}                    
                        
# объявляем задачу с помощью SparkSubmitOperator
spark_submit_base = SparkSubmitOperator(dag=dag_spark, conf = spark_base_config, **base_config)

spark_submit_7 = SparkSubmitOperator(dag=dag_spark, conf = spark_config, **d7_config)

spark_submit_84 = SparkSubmitOperator(dag=dag_spark, conf = spark_config, **d84_config)

spark_submit_base >> [spark_submit_7, spark_submit_84]                        
