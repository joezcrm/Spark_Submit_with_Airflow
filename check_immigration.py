import sys
from pyspark.sql import SparkSession

def check_immigration(spark, input_folder, output_folder):
    # Read original immigration data
    original_df = spark.read.parquet(input_folder + \
        "immigration_data.parquet")
    original_df.createOrReplaceTempView("original")
    
    #Read resulting immigration data
    resulting_df = spark.read.parquet(output_folder + \
        "immigration/fact_immigration.parquet")
    resulting_df.createOrReplaceTempView("resulting")
    
    query = """
        SELECT * FROM
        (
            SELECT cast(cicid AS int) AS id,
                to_date(dtadfile, 'yyyyMMdd') AS record_date
            FROM original
        ) AS orig
        JOIN
        (
            SELECT original_id, record_date FROM resulting
        ) AS result
        ON orig.id = result.original_id
        AND orig.record_date = result.record_date
    """
    df = spark.sql(query)
    if df.count() <= 0:
        raise ValueError(f"Data Quality Check Failed. The file {input_folder} \
            immigration_data.parquet and {output_folder}immigigration/ \
            fact_immigration.parquet have no common data.")
    

def main():
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    check_immigration(spark, "s3://joezcrmdb/data_source/" + subfolder,
        "s3://joezcrmdb/data/final/")
    
if __name__=="__main__":
    main()