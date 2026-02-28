"""
AWS Glue Job: HL7 v2.x Message Parser
Parses HL7 messages from S3 and transforms into Snowflake-ready tabular format.
"""
import sys, json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_INPUT_PATH", "S3_OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def extract_patient_info(hl7_message: str) -> str:
    patient = {}
    for line in hl7_message.strip().split("\n"):
        if line.startswith("PID"):
            fields = line.split("|")
            patient["patient_id"] = fields[3] if len(fields) > 3 else None
            name_parts = fields[5].split("^") if len(fields) > 5 else []
            patient["last_name"] = name_parts[0] if name_parts else None
            patient["first_name"] = name_parts[1] if len(name_parts) > 1 else None
            patient["dob"] = fields[7] if len(fields) > 7 else None
            patient["gender"] = fields[8] if len(fields) > 8 else None
        elif line.startswith("MSH"):
            fields = line.split("|")
            patient["message_timestamp"] = fields[7] if len(fields) > 7 else None
            patient["message_type"] = fields[9] if len(fields) > 9 else None
    patient["processed_at"] = datetime.utcnow().isoformat()
    return json.dumps(patient)


parse_udf = F.udf(extract_patient_info, StringType())
raw_df = spark.read.text(args["S3_INPUT_PATH"])
parsed_df = raw_df.withColumn("parsed", parse_udf(F.col("value")))
parsed_df.write.mode("overwrite").parquet(args["S3_OUTPUT_PATH"])
print(f"Processed {parsed_df.count()} HL7 messages")
job.commit()
