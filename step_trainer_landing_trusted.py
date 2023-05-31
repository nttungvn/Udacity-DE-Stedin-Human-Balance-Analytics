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

# Script generated for node Step trainer landing
Steptrainerlanding_node1685548228653 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlanding_node1685548228653",
)

# Script generated for node Customer curated
Customercurated_node1685549266394 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="customer_curated",
    transformation_ctx="Customercurated_node1685549266394",
)

# Script generated for node Join
Join_node1685543780008 = Join.apply(
    frame1=Steptrainerlanding_node1685548228653,
    frame2=Customercurated_node1685549266394,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1685543780008",
)

# Script generated for node Drop Fields
DropFields_node1685544222066 = DropFields.apply(
    frame=Join_node1685543780008,
    paths=[
        "email",
        "phone",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "lastupdatedate",
        "sharewithfriendsasofdate",
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
