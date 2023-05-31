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

# Script generated for node Customer curated
Customercurated_node1685543749894 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customercurated_node1685543749894",
)

# Script generated for node Step trainer landing
Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tungnt-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlanding_node1",
)

# Script generated for node Join
Join_node1685543780008 = Join.apply(
    frame1=Customercurated_node1685543749894,
    frame2=Steptrainerlanding_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1685543780008",
)

# Script generated for node Drop Fields
DropFields_node1685544222066 = DropFields.apply(
    frame=Join_node1685543780008,
    paths=[
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1685544222066",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1685543863513 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685544222066,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tungnt-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Steptrainertrusted_node1685543863513",
)

job.commit()
