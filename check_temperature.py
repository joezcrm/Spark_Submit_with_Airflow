import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,
                               StructField,
                               DoubleType,
                               IntegerType,
                               DateType,
                               StringType)

def check_temperature(spark, input_folder, output_folder):
    """
    A function to check whether the data processing succeeded
    :param spark, an instance of spark session
    :param input_folder, string type, the directory of data source
    :param output_folder, string type, the output directory of
        data
    """
    # Read original temperature data
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
        "GlobalLandTemperaturesByCity.csv", sep = ";", \
        schema = temp_schema, header = True)
    temp_df.createOrReplaceTempView("original")
    
    # Read resulting temperature data
    result_df = spark.read.parquet(output_folder + \
        "temperature/fact_temperature.parquet")
    result_df.createOrReplaceTempView("result")
    
    query = """
        SELECT * FROM
        (
            SELECT record_date FROM original
            WHERE country = 'United States'
        ) AS orig
        JOIN 
        (
            SELECT record_date FROM result
        ) AS res
        ON orig.record_date = res.record_date
    """
    df = spark.sql(query)
    if df.count() <= 0:
        error_message = "Data Quality Check Failed. The file {} \
            GlobalLandTemperaturesByCity.csv and {} \
            temperature/fact_temperature.parquet have no data in common" \
            .format(input_folder, output_folder)
        raise ValueError(error_message)
    
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
