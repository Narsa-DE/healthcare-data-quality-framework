"""
EHR Data Extractor
Supports HL7 v2, CCD (C-CDA), and FHIR R4 formats
Reads from S3 and normalizes to a common schema
"""

import boto3
import json
import logging
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class EHRRecord:
    patient_id: str
    encounter_id: str
    source_format: str
    raw_payload: Dict
    extracted_at: datetime
    source_system: str


class EHRExtractor:
    """
    Extracts EHR data from S3 in HL7, CCD, and FHIR formats.
    Normalizes records into a common intermediate schema.
    """

    SUPPORTED_FORMATS = ["HL7", "CCD", "FHIR"]

    def __init__(self, source_type: str, s3_bucket: str, s3_prefix: str = "raw/ehr/"):
        if source_type not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {source_type}. Use one of {self.SUPPORTED_FORMATS}")
        self.source_type = source_type
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client("s3")
        self.records: List[EHRRecord] = []

    def extract(self) -> List[EHRRecord]:
        logger.info(f"Starting extraction from s3://{self.s3_bucket}/{self.s3_prefix}")
        objects = self._list_s3_objects()
        for obj_key in objects:
            try:
                raw = self._read_s3_object(obj_key)
                records = self._parse(raw, obj_key)
                self.records.extend(records)
            except Exception as e:
                logger.error(f"Failed to process {obj_key}: {e}")
        logger.info(f"Extracted {len(self.records)} total records")
        return self.records

    def _list_s3_objects(self) -> List[str]:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def _read_s3_object(self, key: str) -> str:
        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def _parse(self, raw: str, source_key: str) -> List[EHRRecord]:
        parsers = {"HL7": self._parse_hl7, "CCD": self._parse_ccd, "FHIR": self._parse_fhir}
        return parsers[self.source_type](raw, source_key)

    def _parse_hl7(self, raw: str, source_key: str) -> List[EHRRecord]:
        records = []
        segments = raw.strip().split("\n")
        patient_id, encounter_id = None, None
        for segment in segments:
            fields = segment.split("|")
            seg_type = fields[0] if fields else ""
            if seg_type == "PID":
                patient_id = fields[3] if len(fields) > 3 else "UNKNOWN"
            elif seg_type == "PV1":
                encounter_id = fields[19] if len(fields) > 19 else "UNKNOWN"
        if patient_id:
            records.append(EHRRecord(
                patient_id=patient_id, encounter_id=encounter_id or "UNKNOWN",
                source_format="HL7", raw_payload={"raw": raw, "source_key": source_key},
                extracted_at=datetime.utcnow(), source_system="HL7_FEED",
            ))
        return records

    def _parse_ccd(self, raw: str, source_key: str) -> List[EHRRecord]:
        import xml.etree.ElementTree as ET
        records = []
        try:
            root = ET.fromstring(raw)
            ns = {"hl7": "urn:hl7-org:v3"}
            patient_id = root.findtext(".//hl7:id", namespaces=ns) or "UNKNOWN"
            records.append(EHRRecord(
                patient_id=patient_id, encounter_id="CCD_" + patient_id,
                source_format="CCD", raw_payload={"source_key": source_key},
                extracted_at=datetime.utcnow(), source_system="CCD_FEED",
            ))
        except ET.ParseError as e:
            logger.error(f"CCD parse error: {e}")
        return records

    def _parse_fhir(self, raw: str, source_key: str) -> List[EHRRecord]:
        records = []
        try:
            bundle = json.loads(raw)
            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                if resource.get("resourceType") == "Patient":
                    patient_id = resource.get("id", "UNKNOWN")
                    records.append(EHRRecord(
                        patient_id=patient_id, encounter_id="FHIR_" + patient_id,
                        source_format="FHIR", raw_payload=resource,
                        extracted_at=datetime.utcnow(), source_system="FHIR_R4",
                    ))
        except json.JSONDecodeError as e:
            logger.error(f"FHIR parse error: {e}")
        return records
