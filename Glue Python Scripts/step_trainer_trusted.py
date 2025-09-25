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

# Script generated for node steptrainer_landing
steptrainer_landing_node1758780504764 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="step_trainer_landing", transformation_ctx="steptrainer_landing_node1758780504764")

# Script generated for node customer_curated
customer_curated_node1758780506271 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="customer_curated", transformation_ctx="customer_curated_node1758780506271")

# Script generated for node SQL Query
SqlQuery3551 = '''
select s.* from s inner join c on c.serialNumber=s.serialNumber

'''
SQLQuery_node1758780638079 = sparkSqlQuery(glueContext, query = SqlQuery3551, mapping = {"s":steptrainer_landing_node1758780504764, "c":customer_curated_node1758780506271}, transformation_ctx = "SQLQuery_node1758780638079")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758780638079, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758780487819", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1758780707128 = glueContext.getSink(path="s3://finaludacityproject/steptrainer/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1758780707128")
step_trainer_trusted_node1758780707128.setCatalogInfo(catalogDatabase="finalproject",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1758780707128.setFormat("json")
step_trainer_trusted_node1758780707128.writeFrame(SQLQuery_node1758780638079)
job.commit()