import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1758779257490 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="customer_trusted", transformation_ctx="customer_trusted_node1758779257490")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1758779256578 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1758779256578")

# Script generated for node SQL Query
SqlQuery3210 = '''
select distinct c.* from a inner join c on a.user=c.email

'''
SQLQuery_node1758779364748 = sparkSqlQuery(glueContext, query = SqlQuery3210, mapping = {"c":customer_trusted_node1758779257490, "a":accelerometer_trusted_node1758779256578}, transformation_ctx = "SQLQuery_node1758779364748")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758779364748, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776373935", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1758779494343 = glueContext.getSink(path="s3://finaludacityproject/customer/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1758779494343")
customer_curated_node1758779494343.setCatalogInfo(catalogDatabase="finalproject",catalogTableName="customer_curated")
customer_curated_node1758779494343.setFormat("json")
customer_curated_node1758779494343.writeFrame(SQLQuery_node1758779364748)
job.commit()