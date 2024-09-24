from pyspark.sql  import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()


schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("stock_symbol", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", FloatType(), True)
])



kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")



client = MongoClient(mongo_uri)
db = client['streaming_db']
print(db.list_collection_names())
collection = db['processed_data']

def write_to_mongo(df, epoch_id):
    if not df.isEmpty():
        records = df.collect()
        for record in records:
            collection.insert_one(record.asDict())


query = parsed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .start()

query.awaitTermination()
spark.sparkContext.setLogLevel("DEBUG")
