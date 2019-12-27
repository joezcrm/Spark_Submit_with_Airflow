import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import (StructType,
                               StructField,
                               DoubleType,
                               IntegerType,
                               DateType,
                               StringType)
import argparse

# udf to process coordinates
@udf (DoubleType())
def to_numeric_lat(text):
    list_result = list(text)
    number_part = list_result[:(len(list_result)-1)]
    if list_result[len(list_result)-1] == "N":
        num_sign = []
    elif list_result[len(list_result)-1] == "S":
        num_sign = ["-"]
    else:
        return None
    return float("".join(num_sign + number_part))

@udf (DoubleType())
def to_numeric_long(text):
    list_result = list(text)
    number_part = list_result[:(len(list_result)-1)]
    if list_result[len(list_result)-1] == "E":
        num_sign = []
    elif list_result[len(list_result)-1] == "W":
        num_sign = ["-"]
    else:
        return None
    return float("".join(num_sign + number_part))

def process_fact_temperature(spark, input_folder, output_folder):
    """
    A function to check whether the data processing succeeded
    :param spark, an instance of spark session
    :param input_folder, string type, the directory of data source
    :param output_folder, string type, the output directory of
        data
    """
    # Read temperature data with custom schema
    temp_schema = StructType([
        StructField("record_date", DateType()),
        StructField("average_temp", DoubleType()),
        StructField("uncertainty", DoubleType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType())
    ])
    temp_df = spark.read.csv(input_folder + \
        "GlobalLandTemperaturesByCity.csv", schema = temp_schema, \
        sep = ";", header = True)
    temp_df = temp_df.withColumn("numeric_lat", to_numeric_lat(col("latitude")))\
        .withColumn("numeric_long", to_numeric_long(col("longitude")))
    temp_df.createOrReplaceTempView("temperature")
    
    # Read cities dimension data
    cities_df = spark.read.parquet(output_folder + \
        "common_dimension/dimension_cities.parquet")
    cities_df.createOrReplaceTempView("cities")
    
    # Create query
    query = """
        SELECT md5(concat(ci.city_id_str, temp.date_str)) AS record_id,
            ci.city_id AS city_id,
            temp.record_date AS record_date,
            temp.average_temp AS average_temp,
            temp.uncertainty As uncertainty
        FROM
        (
            SELECT cast(record_date AS string) AS date_str, record_date, 
                average_temp, uncertainty, city, numeric_lat, numeric_long
            FROM temperature
            WHERE country = 'United States'
        ) AS temp
        JOIN
        (
            SELECT DISTINCT 
                cast(city_id AS string) AS city_id_str, 
                city_id, city, latitude, longitude
            FROM cities
        ) AS ci
        ON temp.city = ci.city
        AND temp.numeric_lat = ci.latitude
        AND temp.numeric_long = ci.longitude
    """
    fact_temperature_df = spark.sql(query)
    fact_temperature_df.write.mode("append").partitionBy("city_id")\
        .parquet(output_folder + "temperature/fact_temperature.parquet")
    
    
def main():
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    process_fact_temperature(spark, "s3://joezcrmdb/data_source/" + subfolder, 
        "s3://joezcrmdb/data/final/")
    
if __name__ == "__main__":
    main()
