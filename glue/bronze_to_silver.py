import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import when, col, regexp_extract, expr, split, to_date, current_timestamp, current_date, lit
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------
# Step 1: Read CSV from S3
# -------------------------
df = spark.read.csv(
    "s3://netflix-data-projec/bronze/netflix_titles_csv.csv/",
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True
)

# -------------------------
# Step 2: Convert to DynamicFrame and back (optional)
# -------------------------
dyf = DynamicFrame.fromDF(df, glueContext, "dyf_from_csv")
df = dyf.toDF()

# -------------------------
# Step 3: Runtime transformation
# -------------------------
df_runtime = df.withColumn(
    "num_part", regexp_extract(col("duration"), r'\d+', 0)
).withColumn(
    "runtime",
    when(col("duration").contains("1 Season"), 50*1)
    .when(col("duration").contains("2 Seasons"), 50*2)
    .when(col("duration").contains("3 Season"), 50*3)
    .when(col("duration").contains("4 Season"), 50*4)
    .when(col("duration").contains("5 Season"), 50*5)
    .when(col("duration").contains("6 Season"), 50*6)
    .when(col("duration").contains("7 Season"), 50*7)
    .when(col("duration").contains("8 Season"), 50*8)
    .when(col("duration").contains("9 Season"), 50*9)
    .when(col("duration").contains("10 Season"), 50*10)
    .when(col("duration").contains("11 Season"), 50*11)
    .when(col("duration").contains("12 Season"), 50*12)
    .when(col("duration").contains("13 Season"), 50*13)
    .when(col("duration").contains("15 Season"), 50*15)
    .when(col("duration").contains("17 Season"), 50*17)
    .when(expr("try_cast(num_part as int)").isNotNull(), expr("try_cast(num_part as int)"))
    .otherwise(None)
).drop("num_part")

# -------------------------
# Step 4: India movie flag
# -------------------------
df_india = df_runtime.withColumn(
    "is_indian_movie",
    when(col("Country").contains("India"), "True").otherwise("False")
)

# -------------------------
# Step 5: Rating extraction
# -------------------------
df_rating = df_india.withColumn("establish_code", split(col("rating"), "-")[0])

# -------------------------
# Step 6: Convert date_added to date
# -------------------------
df_date = df_rating.withColumn(
    "date_converted",
    to_date(col("date_added"), "MMMM d, yyyy")
)

# -------------------------
# Step 7: Add current timestamp, date, and processed_date
# -------------------------
today = datetime.today().strftime("%Y-%m-%d")

df_time = df_date.withColumn("current_time", current_timestamp()) \
    .withColumn("current_date", current_date()) \
    .withColumn("processed_date", lit(today))   # <-- Add processed_date

# -------------------------
# Step 8: Fill null country
# -------------------------
df_fill_values = df_time.fillna({"Country": "Unknown_country"})

# -------------------------
# Step 9: Coalesce to single file
# -------------------------
df_single = df_fill_values.coalesce(1)

# -------------------------
# Step 10: Convert back to DynamicFrame
# -------------------------
dyf_single = DynamicFrame.fromDF(df_single, glueContext, "dyf_single")

# -------------------------
# Step 11: Write to S3 partitioned by processed_date
# -------------------------
glueContext.write_dynamic_frame.from_options(
    frame=dyf_single,
    connection_type="s3",
    connection_options={
        "path": "s3://netflix-data-projec/silver/",
        "partitionKeys": ["processed_date"]   # <-- Partitioning
    },
    format="parquet"
)

# -------------------------
# Step 12: Commit Glue Job
# -------------------------
job.commit()
