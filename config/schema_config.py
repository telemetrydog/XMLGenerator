"""
ACORD TXLifeRequest (TransType 212 - Values Inquiry) schema configuration.

Single source of truth for field definitions, Spark types, XML path mappings,
validation rules, and ACORD tc (type-code) attributes.
"""

from pyspark.sql.types import StructType, StructField, StringType, DateType


ACORD_NAMESPACE = "http://ACORD.org/Standards/Life/2"
ACORD_NS_PREFIX = "ACORD"
TRANS_TYPE_VALUES_INQUIRY = "212"

FIELD_DEFINITIONS = [
    {
        "column_name": "TransRefGUID",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/TransRefGUID",
        "xml_tag": "TransRefGUID",
        "required": True,
        "max_length": 36,
        "regex": r"^[0-9a-fA-F\-]{20,36}$",
        "allowed_values": None,
        "tc_code": None,
        "description": "Unique transaction reference identifier",
    },
    {
        "column_name": "TransType",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/TransType",
        "xml_tag": "TransType",
        "required": True,
        "max_length": 10,
        "regex": None,
        "allowed_values": ["212"],
        "tc_code": "212",
        "tc_description": "Values Inquiry",
        "description": "Transaction type code - 212 for Values Inquiry",
    },
    {
        "column_name": "TransSubType",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/TransSubType",
        "xml_tag": "TransSubType",
        "required": False,
        "max_length": 10,
        "regex": None,
        "allowed_values": ["21201", "21202", "21203", "21204", "21205", "21206", "21207"],
        "tc_code": None,
        "description": "Transaction sub-type code",
    },
    {
        "column_name": "TransExeDate",
        "spark_type": "DATE",
        "xml_path": "TXLifeRequest/TransExeDate",
        "xml_tag": "TransExeDate",
        "required": True,
        "max_length": None,
        "regex": r"^\d{4}-\d{2}-\d{2}$",
        "allowed_values": None,
        "tc_code": None,
        "description": "Transaction execution date (YYYY-MM-DD)",
    },
    {
        "column_name": "TransExeTime",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/TransExeTime",
        "xml_tag": "TransExeTime",
        "required": True,
        "max_length": 8,
        "regex": r"^\d{2}:\d{2}:\d{2}$",
        "allowed_values": None,
        "tc_code": None,
        "description": "Transaction execution time (HH:mm:ss)",
    },
    {
        "column_name": "TransEffDate",
        "spark_type": "DATE",
        "xml_path": "TXLifeRequest/TransEffDate",
        "xml_tag": "TransEffDate",
        "required": False,
        "max_length": None,
        "regex": r"^\d{4}-\d{2}-\d{2}$",
        "allowed_values": None,
        "tc_code": None,
        "description": "Transaction effective date (YYYY-MM-DD)",
    },
    {
        "column_name": "InquiryLevel",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/InquiryLevel",
        "xml_tag": "InquiryLevel",
        "required": False,
        "max_length": 10,
        "regex": None,
        "allowed_values": None,
        "tc_code": None,
        "description": "Depth of inquiry",
    },
    {
        "column_name": "InquiryView",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/InquiryView",
        "xml_tag": "InquiryView",
        "required": False,
        "max_length": 10,
        "regex": None,
        "allowed_values": None,
        "tc_code": None,
        "description": "View type for inquiry",
    },
    {
        "column_name": "NoResponseOK",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/NoResponseOK",
        "xml_tag": "NoResponseOK",
        "required": False,
        "max_length": 1,
        "regex": None,
        "allowed_values": ["0", "1"],
        "tc_code": None,
        "description": "No-response-OK indicator",
    },
    {
        "column_name": "TestIndicator",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/TestIndicator",
        "xml_tag": "TestIndicator",
        "required": False,
        "max_length": 1,
        "regex": None,
        "allowed_values": ["0", "1"],
        "tc_code": None,
        "description": "1=test, 0=production",
    },
    {
        "column_name": "HoldingID",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/@id",
        "xml_tag": "Holding",
        "required": True,
        "max_length": 50,
        "regex": None,
        "allowed_values": None,
        "tc_code": None,
        "is_attribute": True,
        "attribute_name": "id",
        "description": "Holding element id attribute",
    },
    {
        "column_name": "HoldingTypeCode",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/HoldingTypeCode",
        "xml_tag": "HoldingTypeCode",
        "required": True,
        "max_length": 5,
        "regex": None,
        "allowed_values": ["2"],
        "tc_code": "2",
        "tc_description": "Policy",
        "description": "Holding type - 2 for Policy",
    },
    {
        "column_name": "PolNumber",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/PolNumber",
        "xml_tag": "PolNumber",
        "required": True,
        "max_length": 30,
        "regex": r"^[A-Za-z0-9\-]+$",
        "allowed_values": None,
        "tc_code": None,
        "description": "Policy number",
    },
    {
        "column_name": "ProductCode",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/ProductCode",
        "xml_tag": "ProductCode",
        "required": False,
        "max_length": 20,
        "regex": None,
        "allowed_values": None,
        "tc_code": None,
        "description": "Carrier product code",
    },
    {
        "column_name": "CarrierCode",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/CarrierCode",
        "xml_tag": "CarrierCode",
        "required": True,
        "max_length": 10,
        "regex": r"^\d+$",
        "allowed_values": None,
        "tc_code": None,
        "description": "DTCC carrier code",
    },
    {
        "column_name": "LineOfBusiness",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/LineOfBusiness",
        "xml_tag": "LineOfBusiness",
        "required": False,
        "max_length": 5,
        "regex": None,
        "allowed_values": ["1", "2", "3"],
        "tc_code": None,
        "tc_description_map": {"1": "Life", "2": "Annuity", "3": "Health"},
        "description": "Line of business - 2 for Annuity",
    },
    {
        "column_name": "ProductType",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/ProductType",
        "xml_tag": "ProductType",
        "required": False,
        "max_length": 50,
        "regex": None,
        "allowed_values": None,
        "tc_code": None,
        "description": "Annuity product type description",
    },
    {
        "column_name": "PolicyStatus",
        "spark_type": "STRING",
        "xml_path": "TXLifeRequest/OLifE/Holding/Policy/PolicyStatus",
        "xml_tag": "PolicyStatus",
        "required": False,
        "max_length": 10,
        "regex": None,
        "allowed_values": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
        "tc_code": None,
        "tc_description_map": {
            "1": "Active",
            "2": "Inactive",
            "3": "Proposed",
            "4": "Approved",
            "5": "Declined",
            "6": "Terminated",
            "7": "Lapsed",
            "8": "Surrendered",
            "9": "Expired",
            "10": "Pending",
            "11": "Paid Up",
            "12": "Death Claim",
        },
        "description": "Policy status code",
    },
]


SPARK_TYPE_MAP = {
    "STRING": StringType(),
    "DATE": DateType(),
}


def get_spark_schema():
    """Return a PySpark StructType built from FIELD_DEFINITIONS."""
    fields = []
    for fd in FIELD_DEFINITIONS:
        spark_type = SPARK_TYPE_MAP[fd["spark_type"]]
        nullable = not fd["required"]
        fields.append(StructField(fd["column_name"], spark_type, nullable))
    return StructType(fields)


def get_column_names():
    """Return ordered list of column names."""
    return [fd["column_name"] for fd in FIELD_DEFINITIONS]


def get_required_fields():
    """Return list of field definitions where required=True."""
    return [fd for fd in FIELD_DEFINITIONS if fd["required"]]


def get_field_by_column(column_name):
    """Look up a single field definition by column name."""
    for fd in FIELD_DEFINITIONS:
        if fd["column_name"] == column_name:
            return fd
    return None


def get_xml_path_groups():
    """
    Group fields by their XML nesting level for XML construction.
    Returns a dict mapping XML parent paths to lists of field definitions.
    """
    groups = {}
    for fd in FIELD_DEFINITIONS:
        parts = fd["xml_path"].rsplit("/", 1)
        parent = parts[0] if len(parts) > 1 else ""
        groups.setdefault(parent, []).append(fd)
    return groups
