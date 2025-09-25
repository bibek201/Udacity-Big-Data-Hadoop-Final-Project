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

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1758780971037 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="step_trainer_trusted", transformation_ctx="steptrainer_trusted_node1758780971037")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1758780969661 = glueContext.create_dynamic_frame.from_catalog(database="finalproject", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1758780969661")

# Script generated for node SQL Query
SqlQuery3413 = '''
select a.user,a.x,a.y,a.z,s.* from s inner join a on a.timestamp=s.sensorReadingTime

'''
SQLQuery_node1758781103300 = sparkSqlQuery(glueContext, query = SqlQuery3413, mapping = {"a":accelerometer_trusted_node1758780969661, "s":steptrainer_trusted_node1758780971037}, transformation_ctx = "SQLQuery_node1758781103300")

# Script generated for node 
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758781103300, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758780487819", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
_node1758781309030 = glueContext.getSink(path="s3://finaludacityproject/steptrainer/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="_node1758781309030")
_node1758781309030.setCatalogInfo(catalogDatabase="finalproject",catalogTableName="machine_learning_curated")
_node1758781309030.setFormat("json")
_node1758781309030.writeFrame(SQLQuery_node1758781103300)
job.commit()