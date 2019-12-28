# udacity_capstone
## Purpose
This project is to create a data schema based on various data sources and a data pipeline from these sources
using Airflow, Spark, and AWS EMR. Because of Spark, the data pipeline created is able to process a very large data set.
The data is stored in a S3 bucket and, thus, allows simutaneous accesses by a large group of users.
## Prerequisites
1. The EMR created with an EC2 key pair and should support Spark, Hadoop, and Yarn.
2. An inbound traffic for the EC2 master instance under an EMR cluster is authorized, so that Airflow can establish an
connection with the EMR master node.  If it is not been done, follow the [link](
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html) to add a rule for SSH 
inbound traffic.
3. OpenSSH is installed on the Airflow server, so that the server can execute ssh command lines.
4. An Airflow connection is set up, with Host being the master public DNS, Login being "hadoop", and Extra being
>{
>"key_file": "private_key_path.pem"
>}
## Data Source
The data sources include US city demographics information, global airport codes, global monthly average temperature,
and US immigration records.  All the data sources were download from Udacity and uploaded to my s3 bucket for
tesing purpose.  Except changing the format of immiigration records to parquet, the formats of other data sources 
were kept as csv.
## Assumption
This project assumes that the data source format, the data field names, and data field types are constant.
## Data Processing
The python scripts and Jupyter notebook included here will perform the data processing task.  The initial data explorations
are not included here.
### US City Demographics
The US city demographics information is obtained from the US Census and will be updated once a decade. It is suffient to
process the data only once. This data is processed in _capstone.ipynb_. Combining with temperature data, _capstone.ipynb_ will
generate a common dimension file, called _dimension_cities.parquet_. Since the only common data field in the two data sources is
city name and city names are not unique, an adjustment is also made in _capstone.ipynb_.
### Airport Data
The airport data is processed in the python script _fact_airport.py_ by running the script using a spark-submit command. Since the construction of airport is slow, it is suffient to submit the script when needed.
### Temperature and Immigration Data
The scripts _fact_temperature.py_ and _fact_immigration.py_ process temperature and immigration, respectively. Since temperature and
and immigration data can be updated once a day, an Airflow data pipeline is created to submit these scripts with the assumption that 
the data are partitioned first by year, then by month, and by day. To build a correct spark-submit command, the source code of
BashOperator is modified sightly and renamed as SubmitOperator.  Data quality checks of the resulting temperature and immigration data
is also added.
## Data Schema Resulted
### Common Dimension
File Name: **dimension_cities.parquet**

