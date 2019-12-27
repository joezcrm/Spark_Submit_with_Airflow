import sys
from pyspark.sql import SparkSession

# input directory
input_dir = "s3://joezcrmdb/data_source/"
# output directory
output_dir = "s3://joezcrmdb/data/final/"

def check_immigration(spark, input_folder, output_folder):
    """
    A function to check whether the data processing succeeded
    :param spark, an instance of spark session
    :param input_folder, string type, the directory of data source
    :param output_folder, string type, the output directory of
        data proccessing
    """
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
        error_message = "Data Quality Check Failed. The file {} \
            immigration_data.parquet and {}immigigration/ \
            fact_immigration.parquet have no common data." \
            .format(input_folder, outputfolder)
        raise ValueError(error_message)
    

def main():
    # Obtain argument from command line
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    check_immigration(spark, input_dir + subfolder, output_dir)
    
if __name__=="__main__":
    main()
