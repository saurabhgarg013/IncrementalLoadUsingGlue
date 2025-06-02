import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date

#args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'redshift_tmp_dir', 'redshift_cluster', 'database', 'table_prefix', 's3_database', 's3_table'])
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Data Catalog
# datasource = glueContext.create_dynamic_frame.from_catalog(
#     database=args['s3_database'],
#     table_name=args['s3_table'],
#     transformation_ctx="datasource"
# )


input_data = glueContext.create_dynamic_frame.from_catalog(
    database="employees_db",
    table_name="inputfile",
    transformation_ctx="AWSGlueDataCatalog_node1718521018720"
)


# Convert DynamicFrame to DataFrame for transformation
#df = datasource.toDF()

df = input_data.toDF()

# Define the schema for the Redshift table
redshift_schema = {
    "id": "int",
    "gender": "string",
    "bdate": "date",
    "educ": "int",
    "jobcat": "string",
    "salary": "decimal(10,2)",
    "salbegin": "decimal(10,2)",
    "jobtime": "int",
    "prevexp": "int",
    "minority": "string"
}

# Cast columns to appropriate types
for column, dtype in redshift_schema.items():
    if dtype == "int":
        df = df.withColumn(column, df[column].cast("int"))
    elif dtype == "decimal(10,2)":
        df = df.withColumn(column, df[column].cast("decimal(10,2)"))
    elif dtype == "date":
        df = df.withColumn(column, to_date(df[column], "yyyy-MM-dd"))
    else:
        df = df.withColumn(column, df[column].cast("string"))

# Reorder columns to match the Redshift table schema
df = df.select([col for col in redshift_schema.keys()])

# Convert DataFrame back to DynamicFrame
datasource_transformed = DynamicFrame.fromDF(df, glueContext, "datasource_transformed")

# Write transformed data to Redshift
preactions = """
CREATE TABLE IF NOT EXISTS public.employee_cleaned_data_1 (
    id int, gender VARCHAR, bdate date, educ int, jobcat VARCHAR,
    salary decimal(10,2), salbegin decimal(10,2), jobtime int, prevexp int, minority VARCHAR
);
DROP TABLE IF EXISTS public.employee_cleaned_data_1_temp_gzp4bt;
CREATE TABLE public.employee_cleaned_data_1_temp_gzp4bt (
    id int, gender VARCHAR, bdate date, educ int, jobcat VARCHAR,
    salary decimal(10,2), salbegin decimal(10,2), jobtime int, prevexp int, minority VARCHAR
);
"""

postactions = """
BEGIN;
MERGE INTO public.employee_cleaned_data_1 USING public.employee_cleaned_data_1_temp_gzp4bt
ON employee_cleaned_data_1.id = employee_cleaned_data_1_temp_gzp4bt.id
WHEN MATCHED THEN UPDATE SET
    id = employee_cleaned_data_1_temp_gzp4bt.id,
    gender = employee_cleaned_data_1_temp_gzp4bt.gender,
    bdate = employee_cleaned_data_1_temp_gzp4bt.bdate,
    educ = employee_cleaned_data_1_temp_gzp4bt.educ,
    jobcat = employee_cleaned_data_1_temp_gzp4bt.jobcat,
    salary = employee_cleaned_data_1_temp_gzp4bt.salary,
    salbegin = employee_cleaned_data_1_temp_gzp4bt.salbegin,
    jobtime = employee_cleaned_data_1_temp_gzp4bt.jobtime,
    prevexp = employee_cleaned_data_1_temp_gzp4bt.prevexp,
    minority = employee_cleaned_data_1_temp_gzp4bt.minority
WHEN NOT MATCHED THEN INSERT VALUES (
    employee_cleaned_data_1_temp_gzp4bt.id,
    employee_cleaned_data_1_temp_gzp4bt.gender,
    employee_cleaned_data_1_temp_gzp4bt.bdate,
    employee_cleaned_data_1_temp_gzp4bt.educ,
    employee_cleaned_data_1_temp_gzp4bt.jobcat,
    employee_cleaned_data_1_temp_gzp4bt.salary,
    employee_cleaned_data_1_temp_gzp4bt.salbegin,
    employee_cleaned_data_1_temp_gzp4bt.jobtime,
    employee_cleaned_data_1_temp_gzp4bt.prevexp,
    employee_cleaned_data_1_temp_gzp4bt.minority
);
DROP TABLE public.employee_cleaned_data_1_temp_gzp4bt;
END;
"""

AmazonRedshift_node1718527392296 = glueContext.write_dynamic_frame.from_options(
    frame=datasource_transformed,
    connection_type="redshift",
    connection_options={
        "postactions": postactions,
        "redshiftTmpDir": args["TempDir"],
        "useConnectionProperties": "true",
        "dbtable": "public.employee_cleaned_data_1_temp_gzp4bt",
        "connectionName": "Redshift connection",
        "preactions": preactions
    },
    transformation_ctx="AmazonRedshift_node1718527392296"
)

job.commit()


#I need to include logic for move processed file into output folder. so that newfile come inside folder for 
#  incremental logic