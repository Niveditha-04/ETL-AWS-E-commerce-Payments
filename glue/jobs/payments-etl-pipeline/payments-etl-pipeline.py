import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

from pyspark.sql.functions import col, when
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

AmazonS3_node1756448358863 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://payments-etl-pipeline/transactions_clean.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1756448358863"
)

ChangeSchema_node1756448361426 = ApplyMapping.apply(
    frame=AmazonS3_node1756448358863,
    mappings=[
        ("transaction_id", "string", "transaction_id", "string"),
        ("user_id", "string", "user_id", "int"),
        ("amount", "string", "amount", "double"),
        ("currency", "string", "currency", "string"),
        ("timestamp", "string", "timestamp", "timestamp"),
        ("amount_usd", "string", "amount_usd", "double"),
        ("month", "string", "month", "date")
    ],
    transformation_ctx="ChangeSchema_node1756448361426"
)

df = ChangeSchema_node1756448361426.toDF()

df = df.dropna(subset=["transaction_id", "amount", "timestamp"])

df = df.dropDuplicates()

df = df.withColumn("is_anomaly", when(col("amount") > 10000, True).otherwise(False))

final_dynamic_df = DynamicFrame.fromDF(df, glueContext, "final_dynamic_df")

EvaluateDataQuality().process_rows(
    frame=final_dynamic_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1756448191809", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

AmazonS3_node1756448363458 = glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_df, 
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://payments-etl-pipeline/transformed/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1756448363458"
)

job.commit()
