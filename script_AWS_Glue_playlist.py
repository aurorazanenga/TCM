import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
playlist_dataset_path = "s3://zanengadata1/playlist_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
playlist_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(playlist_dataset_path)
    
playlist_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = playlist_dataset.count()
count_items_null = playlist_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")


#============================================================PLAYLIST==========================================================================================

#playlist_dataset_path = "s3://zanengadata1/playlist_dataset.csv"
belong_dataset_path = "s3://zanengadata1/belong_dataset.csv"

playlist_dataset = spark.read.option("header","true").csv(playlist_dataset_path)
belong_dataset = spark.read.option("header","true").csv(belong_dataset_path)

belong_dataset_agg = belong_dataset.groupBy(col("id_playlist").alias("id_cont")).agg(collect_list("id_talk").alias("talk_idx"))
belong_dataset_agg.printSchema()

belong_dataset_agg = playlist_dataset.join(belong_dataset_agg, playlist_dataset.idx == belong_dataset_agg.id_cont, "left") \
    .drop("id_cont") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

belong_dataset_agg.printSchema()

#============================================================PLAYLIST==========================================================================================



mongo_uri = "mongodb://zanengacluster-shard-00-00-ujx3r.mongodb.net:27017,zanengacluster-shard-00-01-ujx3r.mongodb.net:27017,zanengacluster-shard-00-02-ujx3r.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedy_data",
    "username": "aurorazanenga",
    "password": "********",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame

#tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tags_dataset_agg, glueContext, "nested")
#tedx_dataset_dynamic_frame = DynamicFrame.fromDF(watch_next_dataset_agg, glueContext, "nested")
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(belong_dataset_agg, glueContext, "nested")


glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
