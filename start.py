from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import flatten 
import botocore 
import botocore.session
import json
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

BUCKET="dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json"

snowflake_pkgs =  [
    "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
    "net.snowflake:snowflake-jdbc:3.13.3"
]

config = {"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.22","fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df= spark.read.json(f"s3a://{BUCKET}/")


df_changed = df 

#df_changed.show()
#df_changed = df_changed.select(flatten(df_changed.coordinates)).collect()

##df_changed.select(df_changed.col("data.*"))
##df_changed.show()
df_changed = df_changed.select("*", 'coordinates.*')
    
df_changed.printSchema()

client = botocore.session.get_session().create_client('secretsmanager')
cache_config = SecretCacheConfig()
cache = SecretCache( config = cache_config, client = client)
creds = json.loads(cache.get_secret_string('snowflake/capstone/login'))

#print("secret" + creds)
#spark = SparkSession.builder.config(conf = conf).getOrCreate()

#Snowflake username: LOUIS_WW2023
SCHEMA = "LOUIS"
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sfOptions = {
"sfURL": creds["URL"],
"sfPassword": creds["PASSWORD"],
"sfUser": creds["USER_NAME"],
"sfDatabase": creds["DATABASE"],
"sfWarehouse": creds["WAREHOUSE"],
"sfRole": creds["ROLE"], 
"sfSchema": SCHEMA
}

df_changed.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option('dbtable', 'weather').mode('overwrite').save()

