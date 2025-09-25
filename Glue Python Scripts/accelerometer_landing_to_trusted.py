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
customer_trusted_node1758777995423 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="customer_trusted", transformation_ctx="customer_trusted_node1758777995423")

# Script generated for node accelerometer_landing
accelerometer_landing_node1758778049436 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1758778049436")

# Script generated for node SQL Query
SqlQuery3705 = '''
select distinct a.* from a inner join c on a.user=c.email 

'''
SQLQuery_node1758778089609 = sparkSqlQuery(glueContext, query = SqlQuery3705, mapping = {"c":customer_trusted_node1758777995423, "a":accelerometer_landing_node1758778049436}, transformation_ctx = "SQLQuery_node1758778089609")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758778089609, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776373935", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1758778173404 = glueContext.getSink(path="s3://finaludacityproject/accelerometer/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1758778173404")
accelerometer_trusted_node1758778173404.setCatalogInfo(catalogDatabase="finalproject",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1758778173404.setFormat("json")
accelerometer_trusted_node1758778173404.writeFrame(SQLQuery_node1758778089609)
job.commit()