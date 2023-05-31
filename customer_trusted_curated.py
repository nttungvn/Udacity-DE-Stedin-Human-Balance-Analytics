import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer trusted
Customertrusted_node1685542253552 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1685542253552",
)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1685542271874 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlanding_node1685542271874",
)

# Script generated for node Join
Join_node1685542293514 = Join.apply(
    frame1=Customertrusted_node1685542253552,
    frame2=Accelerometerlanding_node1685542271874,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1685542293514",
)

# Script generated for node Drop Fields
DropFields_node1685542332808 = DropFields.apply(
    frame=Join_node1685542293514,
    paths=["y", "z", "timeStamp", "user", "x"],
    transformation_ctx="DropFields_node1685542332808",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685542348899 = DynamicFrame.fromDF(
    DropFields_node1685542332808.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1685542348899",
)

# Script generated for node Customer curated
Customercurated_node1685542362442 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1685542348899,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tungnt-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customercurated_node1685542362442",
)

job.commit()
