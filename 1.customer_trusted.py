import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer landing
Customerlanding_node1685547260568 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="customer_landing",
    transformation_ctx="Customerlanding_node1685547260568",
)

# Script generated for node Filter
Filter_node1685527794372 = Filter.apply(
    frame=Customerlanding_node1685547260568,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1685527794372",
)

# Script generated for node Customer trusted
Customertrusted_node1685546596491 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1685527794372,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tungnt-stedi-lakehouse/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Customertrusted_node1685546596491",
)

job.commit()
