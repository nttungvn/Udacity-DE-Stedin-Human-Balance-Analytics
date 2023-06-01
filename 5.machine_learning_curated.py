
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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1685549407118 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1685549407118",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1685600196092 = glueContext.create_dynamic_frame.from_catalog(
    database="tungnt-stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1685600196092",
)

# Script generated for node Join
Join_node1685600274150 = Join.apply(
    frame1=Accelerometertrusted_node1685549407118,
    frame2=Steptrainertrusted_node1685600196092,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1685600274150",
)

# Script generated for node Drop Fields
DropFields_node1685600288705 = DropFields.apply(
    frame=Join_node1685600274150,
    paths=[
        "sensorreadingtime",
        "serialnumber",
        "distancefromobject",
        "timestamp",
        "x",
        "y",
        "z",
    ],
    transformation_ctx="DropFields_node1685600288705",
)

# Script generated for node Step trainer curated
Steptrainercurated_node1685600349775 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685600288705,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tungnt-stedi-lakehouse/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Steptrainercurated_node1685600349775",
)

job.commit()
