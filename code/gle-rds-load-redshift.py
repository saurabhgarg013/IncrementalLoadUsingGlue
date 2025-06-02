import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node glue_postgre_ data_catalog
glue_postgre_data_catalog_node1740047569073 = glueContext.create_dynamic_frame.from_catalog(database="techno-postgres-database", table_name="postgres_bike_stores_staffs", transformation_ctx="glue_postgre_data_catalog_node1740047569073")

# Script generated for node Amazon Redshift
AmazonRedshift_node1740047653780 = glueContext.write_dynamic_frame.from_options(frame=glue_postgre_data_catalog_node1740047569073, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO dwh.dim_staff USING dwh.dim_staff_temp_8wp2lp ON dim_staff.staff_id = dim_staff_temp_8wp2lp.staff_id WHEN MATCHED THEN UPDATE SET store_id = dim_staff_temp_8wp2lp.store_id, new_col = dim_staff_temp_8wp2lp.new_col, new_col_1 = dim_staff_temp_8wp2lp.new_col_1, phone = dim_staff_temp_8wp2lp.phone, manager_id = dim_staff_temp_8wp2lp.manager_id, staff_id = dim_staff_temp_8wp2lp.staff_id, last_name = dim_staff_temp_8wp2lp.last_name, active = dim_staff_temp_8wp2lp.active, first_name = dim_staff_temp_8wp2lp.first_name, email = dim_staff_temp_8wp2lp.email WHEN NOT MATCHED THEN INSERT VALUES (dim_staff_temp_8wp2lp.store_id, dim_staff_temp_8wp2lp.new_col, dim_staff_temp_8wp2lp.new_col_1, dim_staff_temp_8wp2lp.phone, dim_staff_temp_8wp2lp.manager_id, dim_staff_temp_8wp2lp.staff_id, dim_staff_temp_8wp2lp.last_name, dim_staff_temp_8wp2lp.active, dim_staff_temp_8wp2lp.first_name, dim_staff_temp_8wp2lp.email); DROP TABLE dwh.dim_staff_temp_8wp2lp; END;", "redshiftTmpDir": "s3://aws-glue-assets-905418216032-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "dwh.dim_staff_temp_8wp2lp", "connectionName": "techno_Jdbc_connection", "preactions": "CREATE TABLE IF NOT EXISTS dwh.dim_staff (store_id INTEGER, new_col VARCHAR, new_col_1 VARCHAR, phone VARCHAR, manager_id INTEGER, staff_id INTEGER, last_name VARCHAR, active VARCHAR, first_name VARCHAR, email VARCHAR); DROP TABLE IF EXISTS dwh.dim_staff_temp_8wp2lp; CREATE TABLE dwh.dim_staff_temp_8wp2lp (store_id INTEGER, new_col VARCHAR, new_col_1 VARCHAR, phone VARCHAR, manager_id INTEGER, staff_id INTEGER, last_name VARCHAR, active VARCHAR, first_name VARCHAR, email VARCHAR);"}, transformation_ctx="AmazonRedshift_node1740047653780")

job.commit()