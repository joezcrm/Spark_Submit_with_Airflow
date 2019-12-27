import sys
from pyspark.sql import SparkSession

def process_fact_immigration(spark, input_folder, output_folder):
    """
    A function to check whether the data processing succeeded
    :param spark, an instance of spark session
    :param input_folder, string type, the directory of data source
    :param output_folder, string type, the output directory of
        data
    """
    # Read immigration data
    immigration_df = spark.read.parquet(input_folder + \
        "immigration_data.parquet")
    immigration_df.createOrReplaceTempView("immigration")
    
    # Read port code file
    port_df = spark.read.parquet(output_folder + \
        "immigration/port_code.parquet")
    port_df.createOrReplaceTempView("ports")
    
    # Read transportation mode file
    mode_df = spark.read.parquet(output_folder + \
        "immigration/transportation_mode.parquet")
    mode_df.createOrReplaceTempView("modes")
    
    # Read visa type file
    visa_df = spark.read.parquet(output_folder + \
        "immigration/visa_type.parquet")
    visa_df.createOrReplaceTempView("visa")
    
    # Read common dimension city files
    city_df = spark.read.parquet(output_folder + \
        "common_dimension/dimension_cities.parquet")
    city_df.createOrReplaceTempView("cities")
    
    query = """
        SELECT md5(concat(cast(imm.record_date AS string),
                    cast(imm.id AS string))) AS record_id,
            imm.id AS original_id,
            imm.record_date AS record_date,
            imm.record_year AS record_year,
            imm.record_month AS record_month,
            imm.ins_num AS ins_num,
            imm.admission_number AS admission_number,
            imm.admission_date AS admission_date,
            imm.country_of_citizen_code AS country_of_citizen_code,
            imm.country_of_resident_code AS country_of_resident_code,
            ci.city_id AS port_city_id,
            modes.mode AS transportation_mode,
            visa.visa_type AS visa_category,
            imm.visa_type AS visa_type,
            imm.visa_post AS visa_post,
            imm.birth_year AS birth_year,
            imm.arrival_date AS arrival_date,
            imm.arrival_flag AS arrival_flag,
            imm.occupation AS occupation,
            imm.flight_number AS flight_number,
            imm.airline AS airline,
            imm.departure_date AS departure_date,
            imm.departure_flag AS departure_flag,
            imm.update_flag AS update_flag,
            imm.match_flag AS match_flag
        FROM
        (
        (
        (
        (
            SELECT cast(cicid AS int) AS id,
                to_date(dtadfile, 'yyyyMMdd') AS record_date,
                cast(i94yr AS int) AS record_year,
                cast(i94mon AS int) AS record_month,
                insnum AS ins_num,
                cast(admnum AS int) AS admission_number,
                to_date(dtaddto, 'MMddyyyy') AS admission_date,
                cast(i94cit AS int) AS country_of_citizen_code,
                cast(i94res AS int) AS country_of_resident_code,
                i94port AS port_id,
                UPPER(i94addr) AS up_state,
                cast(i94mode AS int) AS mode_id,
                cast(i94visa AS int) AS visa_id,
                visatype AS visa_type,
                visapost AS visa_post,
                biryear AS birth_year,
                date_add('1960-01-01', cast(arrdate AS int)) AS arrival_date,
                entdepa AS arrival_flag,
                occup AS occupation,
                fltno AS flight_number,
                airline,
                date_add('1960-01-01', cast(depdate AS int)) AS departure_date,
                entdepd AS departure_flag,
                entdepu AS update_flag,
                matflag AS match_flag
            FROM immigration
        ) AS imm
        JOIN modes ON imm.mode_id = modes.mode_id
        )
        JOIN visa ON imm.visa_id = visa.visa_id
        )
        JOIN
        (
            SELECT port_id, UPPER(port_name) AS up_name FROM ports
        ) AS po
        ON imm.port_id = po.port_id
        )
        JOIN 
        (
            SELECT city_id, UPPER(city) AS up_city, UPPER(state_code) AS up_state 
            FROM cities
        ) AS ci
        ON ci.up_city = po.up_name
        AND ci.up_state = imm.up_state      
    """
    final_immigration_df = spark.sql(query)
    final_immigration_df.write.mode("append") \
        .partitionBy("port_city_id", "record_year", "record_month") \
        .parquet(output_folder + "immigration/fact_immigration.parquet")
                                    

def main():
    # Obtain argument from command line
    subfolder = ""
    if len(sys.argv) > 1:
        subfolder = sys.argv[1]
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5") \
        .getOrCreate()
    process_fact_immigration(spark, "s3://joezcrmdb/data_source/" + subfolder, \
        "s3://joezcrmdb/data/final/")

if __name__ == "__main__":
    main()
