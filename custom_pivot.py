import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom transform
def transformvac(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import udf, expr, concat

    df = dfc.select(list(dfc.keys())[0]).toDF()
    cols_to_pivot = df.columns[2:-4]
    pivot_str = "stack({},".format(len(cols_to_pivot))
    for col in cols_to_pivot:
        pivot_str += "'{}','{}',".format(col, col)
    pivot_str += ") as (departamento, Total)"
    pivot_str = pivot_str.replace(",)", ")")

    unPivotDF = df.select(
        "fecha",
        "acumuladas",
        "positivas acumuladas",
        "negativas acumuladas",
        "indeterminadas",
        expr(pivot_str),
    ).where("departamento is not null")

    dyf_unpivot = DynamicFrame.fromDF(unPivotDF, glueContext, "vacunados")
    return DynamicFrameCollection({"CustomTransform0": dyf_unpivot}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="covid", table_name="vacunacion", transformation_ctx="S3bucket_node1"
)

# Script generated for node Custom transform
Customtransform_node1633914362296 = transformvac(
    glueContext, DynamicFrameCollection({"S3bucket_node1": S3bucket_node1}, glueContext)
)

# Script generated for node SelectFromCollection
SelectFromCollection_node1633919036902 = SelectFromCollection.apply(
    dfc=Customtransform_node1633914362296,
    key=list(Customtransform_node1633914362296.keys())[0],
    transformation_ctx="SelectFromCollection_node1633919036902",
)

# Script generated for node Amazon S3
AmazonS3_node1633917978716 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1633919036902,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://rawcmurill5/covid_colombia/vaccinated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1633917978716",
)

job.commit()
