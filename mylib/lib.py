import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
     StringType
)

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(url = "https://github.com/nogibjj/drktao-week10-pyspark/blob/main/data/fifa22.csv",
            file_path = "data/fifa22.csv"):
    """"Extract a url to a file path"""
    with requests.get(url, timeout = 10) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)
    return file_path

def load(spark, data="data/fifa22.csv"):
    schema = StructType([
        StructField("Short_Name", StringType(), True),
        StructField("Overall", IntegerType(), True),
        StructField("Potential", IntegerType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Club_Name", StringType(), True),
        StructField("League_Name", StringType(), True),
        StructField("Club_Position", StringType(), True),
        StructField("Club_Jersey_Number", IntegerType(), True),
        StructField("Nationality_Name", StringType(), True),
        StructField("Preferred_Foot", StringType(), True),
        StructField("Weak_Foot", IntegerType(), True),
        StructField("Skill_Moves", IntegerType(), True),
        StructField("Work_Rates", StringType(), True),
        StructField("Pace", IntegerType(), True),
        StructField("Shooting", IntegerType(), True),
        StructField("Passing", IntegerType(), True),
        StructField("Dribbling", IntegerType(), True),
        StructField("Defending", IntegerType(), True),
        StructField("Physical", IntegerType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    return df

def query(spark, df, name = 'players'): 
    df = df.createOrReplaceTempView(name)
    sql_query = "SELECT * FROM players WHERE Club_Position == 'RW'"

    return spark.sql(sql_query).show()

def data_transform(df):
    df = df.withColumn("Potential_Difference", col("Potential")-col("Overall"))
    return df.show()

