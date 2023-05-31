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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1685547705578 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometerlanding_node1685547705578",
)

# Script generated for node Customer trusted
Customertrusted_node1685547692295 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1685547692295",
)

# Script generated for node Join
Join_node1685528335702 = Join.apply(
    frame1=Customertrusted_node1685547692295,
    frame2=Accelerometerlanding_node1685547705578,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1685528335702",
)

# Script generated for node Drop Fields
DropFields_node1685541767739 = DropFields.apply(
    frame=Join_node1685528335702,
    paths=[
        "email",
        "phone",
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "lastupdatedate",
        "sharewithfriendsasofdate",
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
