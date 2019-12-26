import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,
                               StructField,
                               DoubleType,
                               IntegerType,
                               DateType,
                               StringType)

def check_temperature(spark, input_folder, output_folder):
    # Read original temperature data
    temp_schema = StructType([
        StructField("date", DateType()),
        StructField("average_temp", DoubleType()),
        StructField("uncertainty", DoubleType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType())
    ])
    temp_df = spark.read.csv(input_folder + \
        "GlobalLandTemperaturesByCity.csv", schema = temp_schema, header = True)
    temp_df.createOrReplaceTempView("original")
    
    # Read resulting temperature data
    result_df = spark.read.parquet(output_folder + \
        "temperature/fact_temperature.parquet")
    result_df.createOrReplaceTempView("result")
    
    query = """
        SELECT * FROM
        (
            SELECT date FROM original
        ) AS orig
        JOIN 
        (
            SELECT record_date FROM result
        ) AS res
        ON orig.date = res.record_date
    """
    df = spark.sql(query)
    if df.count() <= 0:
        raise ValueError(f"Data Quality Check Failed. The file {input_folder} \
            GlobalLandTemperaturesByCity.csv and {output_folder} \
            temperature/fact_temperature.parquet have no data in common")

def main():
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    
    check_temperature(spark, "s3://joezcrmdb/data_source/" + subfolder, \
        "s3://joezcrmdb/data/final/")
    
if __name__=="__main__":
    main()