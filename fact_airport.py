import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (StructType,
                               StructField,
                               StringType,
                               IntegerType,
                               DoubleType,
                               DateType)

# udf to process certain columns
@udf (StringType())
def get_state_code(text):
    result = text.split("-")
    return result[1].strip()

@udf (DoubleType())
def get_latitude(text):
    result = text.split(",")
    return float(result[1])

@udf (DoubleType())
def get_longitude(text):
    result = text.split(",")
    return float(result[0])


def process_fact_airport(spark, input_folder, output_folder):
    """
    A function to check whether the data processing succeeded
    :param spark, an instance of spark session
    :param input_folder, string type, the directory of data source
    :param output_folder, string type, the output directory of
        data
    """   
    # Read airport data with custom schema
    airport_schema = StructType([
        StructField("airport_id", StringType()),
        StructField("airport_type", StringType()),
        StructField("airport_name", StringType()),
        StructField("elevation_in_ft", DoubleType()),
        StructField("continent", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("gps_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinate", StringType())
    ])
    airport_df = spark.read.csv(input_folder + "airport-codes_csv.csv", \
        header = True, sep = ",", schema = airport_schema)
    airport_df = airport_df.select(["airport_id", "airport_type", "airport_name", \
            "elevation_in_ft", "country", "region", "city", "gps_code", \
            "iata_code", "local_code", "coordinate"]) \
        .where(airport_df.country == 'US').dropDuplicates()
    airport_df = airport_df \
        .withColumn("state_code", get_state_code(col("region"))) \
        .withColumn("latitude", get_latitude(col("coordinate"))) \
        .withColumn("longitude", get_longitude(col("coordinate")))
    airport_df.createOrReplaceTempView("airport")
    
    # Read cities data
    cities_df = spark.read \
        .parquet(output_folder + "common_dimension/dimension_cities.parquet")
    cities_df.createOrReplaceTempView("dimension")
    
    # Create dataframe to write to files
    query = """
        SELECT DISTINCT
            ap.airport_id AS aiport_id,
            ap.airport_type AS airport_type,
            ap.airport_name AS airport_name,
            ap.elevation_in_ft AS elevation_in_ft,
            ap.gps_code AS gps_code,
            ap.iata_code AS iata_code,
            ap.local_code AS local_code,
            ap.latitude AS latitude,
            ap.longitude AS longitude,
            dm.city_id AS city_id
        FROM
        (
            SELECT airport_id, airport_type, airport_name, elevation_in_ft,
                gps_code, iata_code, local_code, latitude, longitude,
                city, state_code
            FROM airport
        ) AS ap
        JOIN
        (
            SELECT DISTINCT city_id, city, state_code
            FROM dimension
        ) AS dm
        ON ap.city = dm.city
        AND ap.state_code = dm.state_code
    """
    final_airport_df = spark.sql(query)
    final_airport_df.write.mode("overwrite").partitionBy("city_id") \
        .parquet(output_folder + "airports/fact_airports.parquet")
    

def main():
    # Obtain argument from command line
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    process_fact_airport(spark, "s3://joezcrmdb/data_source/" + subfolder, \
        "s3://joezcrmdb/data/final/")
        
if __name__ == "__main__":
    main()
    
