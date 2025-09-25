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

# Script generated for node customer_landing
customer_landing_node1758777078013 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="customer_landing", transformation_ctx="customer_landing_node1758777078013")

# Script generated for node SQL Query
SqlQuery3578 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null

'''
SQLQuery_node1758777153013 = sparkSqlQuery(glueContext, query = SqlQuery3578, mapping = {"myDataSource":customer_landing_node1758777078013}, transformation_ctx = "SQLQuery_node1758777153013")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777153013, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776373935", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758777260795 = glueContext.getSink(path="s3://finaludacityproject/customer/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758777260795")
AmazonS3_node1758777260795.setCatalogInfo(catalogDatabase="finalproject",catalogTableName="customer_trusted")
AmazonS3_node1758777260795.setFormat("json")
AmazonS3_node1758777260795.writeFrame(SQLQuery_node1758777153013)
job.commit()