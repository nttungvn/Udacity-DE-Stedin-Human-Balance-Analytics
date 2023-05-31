import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1685528293508 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/customer/trusted"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1685528293508",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1685528269409 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1685528269409",
)

# Script generated for node Join
Join_node1685528335702 = Join.apply(
    frame1=AccelerometerLanding_node1685528269409,
    frame2=CustomerTrusted_node1685528293508,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1685528335702",
)

# Script generated for node Drop Fields
DropFields_node1685541767739 = DropFields.apply(
    frame=Join_node1685528335702,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1685541767739",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1685528352279 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685541767739,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tungnt-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1685528352279",
)

job.commit()
