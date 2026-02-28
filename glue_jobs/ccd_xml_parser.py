"""
AWS Glue Job: CCD XML Clinical Document Parser
Parses CCD (Continuity of Care Document) XML files from S3.
"""
import sys, json
import xml.etree.ElementTree as ET
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

CDA_NS = {"cda": "urn:hl7-org:v3"}


def parse_ccd_document(xml_content: str) -> str:
    try:
        root = ET.fromstring(xml_content)
        patient = {}
        # Extract patient role
        patient_role = root.find(".//cda:patientRole", CDA_NS)
        if patient_role is not None:
            id_elem = patient_role.find("cda:id", CDA_NS)
            patient["patient_id"] = id_elem.get("extension") if id_elem is not None else None
            name_elem = patient_role.find(".//cda:name", CDA_NS)
            if name_elem is not None:
                given = name_elem.find("cda:given", CDA_NS)
                family = name_elem.find("cda:family", CDA_NS)
                patient["first_name"] = given.text if given is not None else None
                patient["last_name"] = family.text if family is not None else None
        patient["document_type"] = "CCD"
        patient["processed_at"] = datetime.utcnow().isoformat()
        return json.dumps(patient)
    except Exception as e:
        return json.dumps({"error": str(e), "processed_at": datetime.utcnow().isoformat()})


parse_udf = F.udf(parse_ccd_document, StringType())
raw_df = spark.read.text(args["S3_INPUT_PATH"], wholetext=True)
parsed_df = raw_df.withColumn("parsed", parse_udf(F.col("value")))
parsed_df.write.mode("overwrite").parquet(args["S3_OUTPUT_PATH"])
print(f"Processed {parsed_df.count()} CCD documents")
job.commit()
