"""
ACORD TXLifeRequest (TransType 212 / TransSubType 21208 – Withdrawal Quote)
schema configuration.

Derived from:
  - DTCC IFW WSDL  (processValueInquiry21208 operation)
  - ACORD Life & Annuity XSDs  (TXLife, OLifE, Holding, Policy, Arrangement,
    Party, Person, Organization, Relation, Producer, Annuity, TaxWithholding)
  - DTCC IFW Extension schema  (OLifEExtension → AmountQualifier)
  - IFW Web Services Data Dictionary  (required / conditional field rules)

Single source of truth for:
  column definitions, Spark types, XML path mappings, validation rules,
  ACORD tc (type-code) attributes, and DTCC IFW extension fields.
"""

from __future__ import annotations

from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------------------------------------------------------
# Namespace constants
# ---------------------------------------------------------------------------
SOAP_NAMESPACE = "http://www.w3.org/2003/05/soap-envelope"
OPERATION_NAMESPACE = "http://service.iwa.dtcc.com/"
ACORD_NAMESPACE = "http://ACORD.org/Standards/Life/2"
ACORD_NS_PREFIX = "ns3"
OPERATION_NS_PREFIX = "ns2"
SOAP_NS_PREFIX = "soap"

OPERATION_NAME = "processValueInquiry21208"
MESSAGE_TYPE = "VI21208_Msg"
TRANS_TYPE_VALUES_INQUIRY = "212"
TRANS_SUBTYPE_WD_QUOTE = "21208"

# ---------------------------------------------------------------------------
# Field groups  –  logical sections that mirror the XML tree
# ---------------------------------------------------------------------------
GROUP_SOAP = "soap"
GROUP_TXLIFE = "txlife_request"
GROUP_HOLDING = "holding"
GROUP_POLICY = "policy"
GROUP_ARRANGEMENT = "arrangement"
GROUP_TAX_FED = "tax_fed"
GROUP_TAX_STATE = "tax_state"
GROUP_EXTENSION = "olife_extension"
GROUP_PARTY_AGENT = "party_agent"
GROUP_PARTY_DISTRIBUTOR = "party_distributor"
GROUP_PARTY_CARRIER = "party_carrier"
GROUP_PARTY_OWNER = "party_owner"
GROUP_PARTY_ANNUITANT = "party_annuitant"
GROUP_REL_AGENT = "relation_agent"
GROUP_REL_DISTRIBUTOR = "relation_distributor"
GROUP_REL_CARRIER = "relation_carrier"
GROUP_REL_OWNER = "relation_owner"
GROUP_REL_ANNUITANT = "relation_annuitant"
GROUP_META = "metadata"

# ---------------------------------------------------------------------------
# FIELD_DEFINITIONS  –  one entry per CSV column (132 total)
#
# Keys:
#   column_name   : CSV header / Spark column name
#   spark_type    : always "STRING" (all CSV data is text)
#   group         : logical XML section
#   xml_tag       : local element / attribute name in the XML tree
#   required      : True  = required per DTCC IFW Data Dictionary
#                   False = optional or conditional
#   conditional   : human-readable condition (empty string if always required)
#   tc_code       : fixed tc attribute value ("" if not fixed)
#   description   : human-readable field purpose
#   max_length    : max character length (0 = unlimited)
#   regex         : validation regex ("" = none)
#   allowed_values: list of valid tc / text values (empty list = any)
# ---------------------------------------------------------------------------

FIELD_DEFINITIONS: list[dict] = [
    # ── SOAP / Message wrapper ──────────────────────────────────────────
    {"column_name": "SoapEnvelopeNs",              "spark_type": "STRING", "group": GROUP_SOAP, "xml_tag": "Envelope",              "required": True,  "conditional": "",  "tc_code": "", "description": "SOAP 1.2 envelope namespace URI",                    "max_length": 0, "regex": "", "allowed_values": ["http://www.w3.org/2003/05/soap-envelope"]},
    {"column_name": "SoapBodyPresent",              "spark_type": "STRING", "group": GROUP_SOAP, "xml_tag": "Body",                  "required": True,  "conditional": "",  "tc_code": "", "description": "SOAP Body presence flag (1=yes)",                     "max_length": 1, "regex": "^[01]$", "allowed_values": ["1"]},
    {"column_name": "OperationName",                "spark_type": "STRING", "group": GROUP_SOAP, "xml_tag": "processValueInquiry21208", "required": True,  "conditional": "",  "tc_code": "", "description": "WSDL operation name",                                 "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "OperationNs",                  "spark_type": "STRING", "group": GROUP_SOAP, "xml_tag": "operationNs",            "required": True,  "conditional": "",  "tc_code": "", "description": "WSDL operation namespace",                            "max_length": 0, "regex": "", "allowed_values": ["http://service.iwa.dtcc.com/"]},
    {"column_name": "MessageType",                  "spark_type": "STRING", "group": GROUP_SOAP, "xml_tag": "VI21208_Msg",            "required": True,  "conditional": "",  "tc_code": "", "description": "IFW message wrapper element name",                     "max_length": 20, "regex": "", "allowed_values": []},

    # ── TXLifeRequest ───────────────────────────────────────────────────
    {"column_name": "TXLifeRequest_PrimaryObjectID", "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TXLifeRequest",       "required": True,  "conditional": "",  "tc_code": "", "description": "PrimaryObjectID attribute on TXLifeRequest",          "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "TransRefGUID",                 "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransRefGUID",          "required": True,  "conditional": "",  "tc_code": "", "description": "Unique transaction reference identifier",               "max_length": 36, "regex": "^[0-9a-zA-Z\\-]{20,36}$", "allowed_values": []},
    {"column_name": "TransType_tc",                 "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransType",             "required": True,  "conditional": "",  "tc_code": "212", "description": "Transaction type tc – 212 (Values Inquiry)",          "max_length": 10, "regex": "", "allowed_values": ["212"]},
    {"column_name": "TransType_text",               "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransType_text",        "required": True,  "conditional": "",  "tc_code": "", "description": "TransType element text",                               "max_length": 50, "regex": "", "allowed_values": ["Values Inquiry"]},
    {"column_name": "TransSubType_tc",              "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransSubType",          "required": True,  "conditional": "",  "tc_code": "21208", "description": "Transaction sub-type tc – 21208 (Withdrawal Quote)",  "max_length": 10, "regex": "", "allowed_values": ["21208"]},
    {"column_name": "TransSubType_text",            "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransSubType_text",     "required": True,  "conditional": "",  "tc_code": "", "description": "TransSubType element text",                            "max_length": 50, "regex": "", "allowed_values": ["Withdrawal Quote"]},
    {"column_name": "TransExeDate",                 "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransExeDate",          "required": True,  "conditional": "",  "tc_code": "", "description": "Transaction execution date (YYYY-MM-DD)",              "max_length": 10, "regex": "^\\d{4}-\\d{2}-\\d{2}$", "allowed_values": []},
    {"column_name": "TransExeTime",                 "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "TransExeTime",          "required": True,  "conditional": "",  "tc_code": "", "description": "Transaction execution time with timezone",              "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "PendingResponseOK_tc",         "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "PendingResponseOK",    "required": True,  "conditional": "",  "tc_code": "0", "description": "PendingResponseOK tc – 0 (False)",                   "max_length": 5,  "regex": "", "allowed_values": ["0", "1"]},
    {"column_name": "PendingResponseOK_text",       "spark_type": "STRING", "group": GROUP_TXLIFE, "xml_tag": "PendingResponseOK_text", "required": True,  "conditional": "",  "tc_code": "", "description": "PendingResponseOK element text",                      "max_length": 10, "regex": "", "allowed_values": ["False", "True"]},

    # ── Holding ─────────────────────────────────────────────────────────
    {"column_name": "Holding_id",                   "spark_type": "STRING", "group": GROUP_HOLDING, "xml_tag": "Holding",              "required": True,  "conditional": "",  "tc_code": "", "description": "Holding element id attribute",                         "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "HoldingTypeCode_tc",           "spark_type": "STRING", "group": GROUP_HOLDING, "xml_tag": "HoldingTypeCode",      "required": True,  "conditional": "",  "tc_code": "2", "description": "HoldingTypeCode tc – 2 (Policy)",                    "max_length": 5,  "regex": "", "allowed_values": ["2"]},
    {"column_name": "HoldingTypeCode_text",         "spark_type": "STRING", "group": GROUP_HOLDING, "xml_tag": "HoldingTypeCode_text", "required": True,  "conditional": "",  "tc_code": "", "description": "HoldingTypeCode element text",                        "max_length": 20, "regex": "", "allowed_values": ["Policy"]},
    {"column_name": "DistributorClientAcctNum",     "spark_type": "STRING", "group": GROUP_HOLDING, "xml_tag": "DistributorClientAcctNum", "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Distributor client account number", "max_length": 30, "regex": "", "allowed_values": []},

    # ── Policy ──────────────────────────────────────────────────────────
    {"column_name": "Policy_CarrierPartyID",        "spark_type": "STRING", "group": GROUP_POLICY, "xml_tag": "Policy",                "required": True,  "conditional": "",  "tc_code": "", "description": "Policy CarrierPartyID attribute",                     "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "PolNumber",                    "spark_type": "STRING", "group": GROUP_POLICY, "xml_tag": "PolNumber",              "required": True,  "conditional": "",  "tc_code": "", "description": "Policy number",                                       "max_length": 30, "regex": "^[A-Za-z0-9\\-]+$", "allowed_values": []},
    {"column_name": "Policy_CarrierCode",           "spark_type": "STRING", "group": GROUP_POLICY, "xml_tag": "CarrierCode",            "required": True,  "conditional": "",  "tc_code": "", "description": "DTCC carrier code within Policy",                      "max_length": 10, "regex": "^\\d+$", "allowed_values": []},
    {"column_name": "CusipNum",                     "spark_type": "STRING", "group": GROUP_POLICY, "xml_tag": "CusipNum",               "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "CUSIP number",                     "max_length": 15, "regex": "", "allowed_values": []},
    {"column_name": "Annuity_present",              "spark_type": "STRING", "group": GROUP_POLICY, "xml_tag": "Annuity",                "required": True,  "conditional": "",  "tc_code": "", "description": "Empty Annuity element flag (1=present)",              "max_length": 1,  "regex": "^[01]$", "allowed_values": ["0", "1"]},

    # ── Arrangement ─────────────────────────────────────────────────────
    {"column_name": "ArrMode_tc",                   "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrMode",           "required": True,  "conditional": "",  "tc_code": "", "description": "Arrangement mode tc",                                 "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "ArrMode_text",                 "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrMode_text",      "required": True,  "conditional": "",  "tc_code": "", "description": "ArrMode element text",                                "max_length": 30, "regex": "", "allowed_values": []},
    {"column_name": "ArrType_tc",                   "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrType",           "required": True,  "conditional": "",  "tc_code": "", "description": "Arrangement type tc",                                 "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "ArrType_text",                 "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrType_text",      "required": True,  "conditional": "",  "tc_code": "", "description": "ArrType element text",                                "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "ArrSubType_tc",                "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrSubType",        "required": True,  "conditional": "",  "tc_code": "", "description": "Arrangement sub-type tc",                             "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "ArrSubType_text",              "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ArrSubType_text",   "required": True,  "conditional": "",  "tc_code": "", "description": "ArrSubType element text",                             "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "ModalAmt",                     "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "ModalAmt",          "required": False, "conditional": "Required when ArrSubType tc=4 (Specified Amount)", "tc_code": "", "description": "Withdrawal modal amount", "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "SourceTransferAmtType_tc",     "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "SourceTransferAmtType",     "required": True,  "conditional": "",  "tc_code": "", "description": "Source transfer amount type tc",                    "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "SourceTransferAmtType_text",   "spark_type": "STRING", "group": GROUP_ARRANGEMENT, "xml_tag": "SourceTransferAmtType_text", "required": True,  "conditional": "",  "tc_code": "", "description": "SourceTransferAmtType element text",                 "max_length": 50, "regex": "", "allowed_values": []},

    # ── TaxWithholding – Federal ────────────────────────────────────────
    {"column_name": "TaxFed_id",                    "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholding",         "required": True,  "conditional": "",  "tc_code": "", "description": "Federal TaxWithholding id attribute",                 "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "TaxFed_AppliesToPartyID",      "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholding_AppliesTo", "required": True,  "conditional": "",  "tc_code": "", "description": "Federal TaxWithholding AppliesToPartyID attribute",  "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "TaxFed_Place_tc",              "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholdingPlace",    "required": True,  "conditional": "",  "tc_code": "1", "description": "Federal TaxWithholdingPlace tc – 1 (Federal)",       "max_length": 5,  "regex": "", "allowed_values": ["1"]},
    {"column_name": "TaxFed_Place_text",            "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholdingPlace_text", "required": True,  "conditional": "",  "tc_code": "", "description": "TaxWithholdingPlace text for Federal",               "max_length": 20, "regex": "", "allowed_values": ["Federal"]},
    {"column_name": "TaxFed_Type_tc",               "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholdingType",     "required": True,  "conditional": "",  "tc_code": "", "description": "Federal TaxWithholdingType tc",                      "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "TaxFed_Type_text",             "spark_type": "STRING", "group": GROUP_TAX_FED, "xml_tag": "TaxWithholdingType_text",  "required": True,  "conditional": "",  "tc_code": "", "description": "Federal TaxWithholdingType text",                    "max_length": 20, "regex": "", "allowed_values": []},

    # ── TaxWithholding – State ──────────────────────────────────────────
    {"column_name": "TaxState_id",                  "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholding",          "required": True,  "conditional": "",  "tc_code": "", "description": "State TaxWithholding id attribute",                   "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "TaxState_AppliesToPartyID",    "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholding_AppliesTo", "required": True,  "conditional": "",  "tc_code": "", "description": "State TaxWithholding AppliesToPartyID attribute",    "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "TaxState_Place_tc",            "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholdingPlace",     "required": True,  "conditional": "",  "tc_code": "2", "description": "State TaxWithholdingPlace tc – 2 (State)",          "max_length": 5,  "regex": "", "allowed_values": ["2"]},
    {"column_name": "TaxState_Place_text",          "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholdingPlace_text", "required": True,  "conditional": "",  "tc_code": "", "description": "TaxWithholdingPlace text for State",                 "max_length": 20, "regex": "", "allowed_values": ["State"]},
    {"column_name": "TaxState_Type_tc",             "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholdingType",      "required": True,  "conditional": "",  "tc_code": "", "description": "State TaxWithholdingType tc",                        "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "TaxState_Type_text",           "spark_type": "STRING", "group": GROUP_TAX_STATE, "xml_tag": "TaxWithholdingType_text",  "required": True,  "conditional": "",  "tc_code": "", "description": "State TaxWithholdingType text",                      "max_length": 20, "regex": "", "allowed_values": []},

    # ── OLifEExtension (DTCC IFW) ───────────────────────────────────────
    {"column_name": "OLifEExtension_VendorCode",    "spark_type": "STRING", "group": GROUP_EXTENSION, "xml_tag": "OLifEExtension",          "required": False, "conditional": "Required when AmountQualifier present", "tc_code": "", "description": "OLifEExtension VendorCode attribute", "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "OLifEExtension_ExtensionCode", "spark_type": "STRING", "group": GROUP_EXTENSION, "xml_tag": "OLifEExtension_ext",      "required": False, "conditional": "Required when AmountQualifier present", "tc_code": "", "description": "OLifEExtension ExtensionCode attribute", "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "AmountQualifier_tc",           "spark_type": "STRING", "group": GROUP_EXTENSION, "xml_tag": "AmountQualifier",         "required": False, "conditional": "Required when ArrSubType tc=4",         "tc_code": "", "description": "Amount qualifier tc (Gross/Net)",        "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "AmountQualifier_text",         "spark_type": "STRING", "group": GROUP_EXTENSION, "xml_tag": "AmountQualifier_text",    "required": False, "conditional": "Required when ArrSubType tc=4",         "tc_code": "", "description": "AmountQualifier element text",            "max_length": 20, "regex": "", "allowed_values": []},

    # ── Party – Agent ───────────────────────────────────────────────────
    {"column_name": "Party_Agent_id",                       "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "Party",              "required": True,  "conditional": "",  "tc_code": "", "description": "Agent Party id attribute",                      "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_PartyTypeCode_tc",         "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "PartyTypeCode",      "required": True,  "conditional": "",  "tc_code": "", "description": "Agent PartyTypeCode tc (1=Person)",              "max_length": 5,  "regex": "", "allowed_values": ["1", "2"]},
    {"column_name": "Party_Agent_PartyTypeCode_text",       "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "PartyTypeCode_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Agent PartyTypeCode text",                     "max_length": 20, "regex": "", "allowed_values": ["Person", "Organization"]},
    {"column_name": "Party_Agent_FirstName",                "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "FirstName",          "required": True,  "conditional": "",  "tc_code": "", "description": "Agent first name",                              "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_LastName",                 "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "LastName",           "required": True,  "conditional": "",  "tc_code": "", "description": "Agent last name",                               "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_NIPRNumber",               "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "NIPRNumber",         "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Agent NIPR number",                "max_length": 20, "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_IDPart",                   "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "IdentificationPart", "required": True,  "conditional": "",  "tc_code": "", "description": "Agent partial ID (last 4 SSN)",                "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_PartialIDType_tc",         "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "PartialIDType",      "required": True,  "conditional": "",  "tc_code": "", "description": "Agent PartialIDType tc",                         "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "Party_Agent_PartialIDType_text",       "spark_type": "STRING", "group": GROUP_PARTY_AGENT, "xml_tag": "PartialIDType_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Agent PartialIDType text",                        "max_length": 20, "regex": "", "allowed_values": []},

    # ── Party – Distributor ─────────────────────────────────────────────
    {"column_name": "Party_Distributor_id",                         "spark_type": "STRING", "group": GROUP_PARTY_DISTRIBUTOR, "xml_tag": "Party",                  "required": True,  "conditional": "",  "tc_code": "", "description": "Distributor Party id attribute",                "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_Distributor_PartyTypeCode_tc",           "spark_type": "STRING", "group": GROUP_PARTY_DISTRIBUTOR, "xml_tag": "PartyTypeCode",          "required": True,  "conditional": "",  "tc_code": "2", "description": "Distributor PartyTypeCode tc (2=Organization)",   "max_length": 5,  "regex": "", "allowed_values": ["2"]},
    {"column_name": "Party_Distributor_PartyTypeCode_text",         "spark_type": "STRING", "group": GROUP_PARTY_DISTRIBUTOR, "xml_tag": "PartyTypeCode_text",     "required": True,  "conditional": "",  "tc_code": "", "description": "Distributor PartyTypeCode text",               "max_length": 20, "regex": "", "allowed_values": ["Organization"]},
    {"column_name": "Party_Distributor_DTCCMemberCode",             "spark_type": "STRING", "group": GROUP_PARTY_DISTRIBUTOR, "xml_tag": "DTCCMemberCode",         "required": True,  "conditional": "",  "tc_code": "", "description": "Distributor DTCC member code",                 "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_Distributor_DTCCAssociatedMemberCode",   "spark_type": "STRING", "group": GROUP_PARTY_DISTRIBUTOR, "xml_tag": "DTCCAssociatedMemberCode", "required": True,  "conditional": "",  "tc_code": "", "description": "Distributor DTCC associated member code",       "max_length": 10, "regex": "", "allowed_values": []},

    # ── Party – Carrier ─────────────────────────────────────────────────
    {"column_name": "Party_Carrier_id",                         "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "Party",                  "required": True,  "conditional": "",  "tc_code": "", "description": "Carrier Party id attribute",                  "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_Carrier_PartyTypeCode_tc",           "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "PartyTypeCode",          "required": True,  "conditional": "",  "tc_code": "2", "description": "Carrier PartyTypeCode tc (2=Organization)",     "max_length": 5,  "regex": "", "allowed_values": ["2"]},
    {"column_name": "Party_Carrier_PartyTypeCode_text",         "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "PartyTypeCode_text",     "required": True,  "conditional": "",  "tc_code": "", "description": "Carrier PartyTypeCode text",                 "max_length": 20, "regex": "", "allowed_values": ["Organization"]},
    {"column_name": "Party_Carrier_DTCCMemberCode",             "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "DTCCMemberCode",         "required": True,  "conditional": "",  "tc_code": "", "description": "Carrier DTCC member code",                   "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_Carrier_DTCCAssociatedMemberCode",   "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "DTCCAssociatedMemberCode", "required": True,  "conditional": "",  "tc_code": "", "description": "Carrier DTCC associated member code",         "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_Carrier_CarrierCode",               "spark_type": "STRING", "group": GROUP_PARTY_CARRIER, "xml_tag": "CarrierCode",            "required": True,  "conditional": "",  "tc_code": "", "description": "Carrier code within Carrier Party element",   "max_length": 10, "regex": "^\\d+$", "allowed_values": []},

    # ── Party – Primary Owner ───────────────────────────────────────────
    {"column_name": "Party_PrimaryOwner_id",                    "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "Party",              "required": True,  "conditional": "",  "tc_code": "", "description": "Primary Owner Party id attribute",             "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryOwner_PartyTypeCode_tc",      "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "PartyTypeCode",      "required": True,  "conditional": "",  "tc_code": "", "description": "Owner PartyTypeCode tc",                        "max_length": 5,  "regex": "", "allowed_values": ["1", "2"]},
    {"column_name": "Party_PrimaryOwner_PartyTypeCode_text",    "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "PartyTypeCode_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Owner PartyTypeCode text",                       "max_length": 20, "regex": "", "allowed_values": ["Person", "Organization"]},
    {"column_name": "Party_PrimaryOwner_FirstName",             "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "FirstName",          "required": True,  "conditional": "",  "tc_code": "", "description": "Owner first name",                             "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryOwner_LastName",              "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "LastName",           "required": True,  "conditional": "",  "tc_code": "", "description": "Owner last name",                              "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryOwner_IDPart",                "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "IdentificationPart", "required": True,  "conditional": "",  "tc_code": "", "description": "Owner partial ID (last 4 SSN)",               "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryOwner_PartialIDType_tc",      "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "PartialIDType",      "required": True,  "conditional": "",  "tc_code": "", "description": "Owner PartialIDType tc",                        "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryOwner_PartialIDType_text",    "spark_type": "STRING", "group": GROUP_PARTY_OWNER, "xml_tag": "PartialIDType_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Owner PartialIDType text",                       "max_length": 20, "regex": "", "allowed_values": []},

    # ── Party – Primary Annuitant ───────────────────────────────────────
    {"column_name": "Party_PrimaryAnnuitant_id",                    "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "Party",              "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Primary Annuitant Party id attribute",       "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryAnnuitant_PartyTypeCode_tc",      "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "PartyTypeCode",      "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant PartyTypeCode tc",                  "max_length": 5,  "regex": "", "allowed_values": ["1", "2"]},
    {"column_name": "Party_PrimaryAnnuitant_PartyTypeCode_text",    "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "PartyTypeCode_text", "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant PartyTypeCode text",                 "max_length": 20, "regex": "", "allowed_values": ["Person", "Organization"]},
    {"column_name": "Party_PrimaryAnnuitant_FirstName",             "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "FirstName",          "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant first name",                        "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryAnnuitant_LastName",              "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "LastName",           "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant last name",                         "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryAnnuitant_IDPart",                "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "IdentificationPart", "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant partial ID (last 4 SSN)",           "max_length": 10, "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryAnnuitant_PartialIDType_tc",      "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "PartialIDType",      "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant PartialIDType tc",                  "max_length": 5,  "regex": "", "allowed_values": []},
    {"column_name": "Party_PrimaryAnnuitant_PartialIDType_text",    "spark_type": "STRING", "group": GROUP_PARTY_ANNUITANT, "xml_tag": "PartialIDType_text", "required": False, "conditional": "Conditional per carrier", "tc_code": "", "description": "Annuitant PartialIDType text",                 "max_length": 20, "regex": "", "allowed_values": []},

    # ── Relation – Agent ────────────────────────────────────────────────
    {"column_name": "Relation_Agent_id",                        "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "Relation",                "required": True,  "conditional": "",  "tc_code": "", "description": "Agent Relation id attribute",                 "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Agent_OriginatingObjectID",       "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "OriginatingObjectID",   "required": True,  "conditional": "",  "tc_code": "", "description": "Agent Relation OriginatingObjectID attribute", "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Agent_RelatedObjectID",           "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "RelatedObjectID",       "required": True,  "conditional": "",  "tc_code": "", "description": "Agent Relation RelatedObjectID attribute",     "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Agent_OriginatingObjectType_tc",  "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "OriginatingObjectType", "required": True,  "conditional": "",  "tc_code": "4", "description": "Agent OriginatingObjectType tc – 4 (Holding)",   "max_length": 5,  "regex": "", "allowed_values": ["4"]},
    {"column_name": "Relation_Agent_OriginatingObjectType_text", "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "OriginatingObjectType_text", "required": True, "conditional": "", "tc_code": "", "description": "Agent OriginatingObjectType text",             "max_length": 20, "regex": "", "allowed_values": ["Holding"]},
    {"column_name": "Relation_Agent_RelatedObjectType_tc",      "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "RelatedObjectType",     "required": True,  "conditional": "",  "tc_code": "6", "description": "Agent RelatedObjectType tc – 6 (Party)",       "max_length": 5,  "regex": "", "allowed_values": ["6"]},
    {"column_name": "Relation_Agent_RelatedObjectType_text",    "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "RelatedObjectType_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Agent RelatedObjectType text",                 "max_length": 20, "regex": "", "allowed_values": ["Party"]},
    {"column_name": "Relation_Agent_RoleCode_tc",               "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "RelationRoleCode",      "required": True,  "conditional": "",  "tc_code": "38", "description": "Agent RelationRoleCode tc – 38 (Servicing Agent)", "max_length": 5, "regex": "", "allowed_values": ["38"]},
    {"column_name": "Relation_Agent_RoleCode_text",             "spark_type": "STRING", "group": GROUP_REL_AGENT, "xml_tag": "RelationRoleCode_text", "required": True,  "conditional": "",  "tc_code": "", "description": "Agent RelationRoleCode text",                   "max_length": 30, "regex": "", "allowed_values": ["Servicing Agent"]},

    # ── Relation – Distributor ──────────────────────────────────────────
    {"column_name": "Relation_Distributor_id",                        "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "Relation",                "required": True,  "conditional": "",  "tc_code": "",  "description": "Distributor Relation id attribute",                 "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Distributor_OriginatingObjectID",       "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "OriginatingObjectID",   "required": True,  "conditional": "",  "tc_code": "",  "description": "Distributor Relation OriginatingObjectID",         "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Distributor_RelatedObjectID",           "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "RelatedObjectID",       "required": True,  "conditional": "",  "tc_code": "",  "description": "Distributor Relation RelatedObjectID",             "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Distributor_OriginatingObjectType_tc",  "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "OriginatingObjectType", "required": True,  "conditional": "",  "tc_code": "4", "description": "Distributor OriginatingObjectType tc – 4 (Holding)", "max_length": 5,  "regex": "", "allowed_values": ["4"]},
    {"column_name": "Relation_Distributor_OriginatingObjectType_text", "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "OriginatingObjectType_text", "required": True, "conditional": "", "tc_code": "", "description": "Distributor OriginatingObjectType text",           "max_length": 20, "regex": "", "allowed_values": ["Holding"]},
    {"column_name": "Relation_Distributor_RelatedObjectType_tc",      "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "RelatedObjectType",     "required": True,  "conditional": "",  "tc_code": "6", "description": "Distributor RelatedObjectType tc – 6 (Party)",      "max_length": 5,  "regex": "", "allowed_values": ["6"]},
    {"column_name": "Relation_Distributor_RelatedObjectType_text",    "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "RelatedObjectType_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Distributor RelatedObjectType text",                "max_length": 20, "regex": "", "allowed_values": ["Party"]},
    {"column_name": "Relation_Distributor_RoleCode_tc",               "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "RelationRoleCode",      "required": True,  "conditional": "",  "tc_code": "83", "description": "Distributor RelationRoleCode tc – 83 (Broker Dealer)", "max_length": 5, "regex": "", "allowed_values": ["83"]},
    {"column_name": "Relation_Distributor_RoleCode_text",             "spark_type": "STRING", "group": GROUP_REL_DISTRIBUTOR, "xml_tag": "RelationRoleCode_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Distributor RelationRoleCode text",                  "max_length": 30, "regex": "", "allowed_values": ["Broker Dealer"]},

    # ── Relation – Carrier ──────────────────────────────────────────────
    {"column_name": "Relation_Carrier_id",                        "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "Relation",                "required": True,  "conditional": "",  "tc_code": "",  "description": "Carrier Relation id attribute",                  "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Carrier_OriginatingObjectID",       "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "OriginatingObjectID",   "required": True,  "conditional": "",  "tc_code": "",  "description": "Carrier Relation OriginatingObjectID",          "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Carrier_RelatedObjectID",           "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "RelatedObjectID",       "required": True,  "conditional": "",  "tc_code": "",  "description": "Carrier Relation RelatedObjectID",              "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Carrier_OriginatingObjectType_tc",  "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "OriginatingObjectType", "required": True,  "conditional": "",  "tc_code": "4", "description": "Carrier OriginatingObjectType tc – 4 (Holding)",  "max_length": 5,  "regex": "", "allowed_values": ["4"]},
    {"column_name": "Relation_Carrier_OriginatingObjectType_text", "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "OriginatingObjectType_text", "required": True, "conditional": "", "tc_code": "", "description": "Carrier OriginatingObjectType text",            "max_length": 20, "regex": "", "allowed_values": ["Holding"]},
    {"column_name": "Relation_Carrier_RelatedObjectType_tc",      "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "RelatedObjectType",     "required": True,  "conditional": "",  "tc_code": "6", "description": "Carrier RelatedObjectType tc – 6 (Party)",       "max_length": 5,  "regex": "", "allowed_values": ["6"]},
    {"column_name": "Relation_Carrier_RelatedObjectType_text",    "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "RelatedObjectType_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Carrier RelatedObjectType text",                 "max_length": 20, "regex": "", "allowed_values": ["Party"]},
    {"column_name": "Relation_Carrier_RoleCode_tc",               "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "RelationRoleCode",      "required": True,  "conditional": "",  "tc_code": "87", "description": "Carrier RelationRoleCode tc – 87 (Carrier)",      "max_length": 5,  "regex": "", "allowed_values": ["87"]},
    {"column_name": "Relation_Carrier_RoleCode_text",             "spark_type": "STRING", "group": GROUP_REL_CARRIER, "xml_tag": "RelationRoleCode_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Carrier RelationRoleCode text",                   "max_length": 30, "regex": "", "allowed_values": ["Carrier"]},

    # ── Relation – Owner ────────────────────────────────────────────────
    {"column_name": "Relation_Owner_id",                        "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "Relation",                "required": True,  "conditional": "",  "tc_code": "",  "description": "Owner Relation id attribute",                  "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Owner_OriginatingObjectID",       "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "OriginatingObjectID",   "required": True,  "conditional": "",  "tc_code": "",  "description": "Owner Relation OriginatingObjectID",            "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Owner_RelatedObjectID",           "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "RelatedObjectID",       "required": True,  "conditional": "",  "tc_code": "",  "description": "Owner Relation RelatedObjectID",                "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Owner_OriginatingObjectType_tc",  "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "OriginatingObjectType", "required": True,  "conditional": "",  "tc_code": "4", "description": "Owner OriginatingObjectType tc – 4 (Holding)",   "max_length": 5,  "regex": "", "allowed_values": ["4"]},
    {"column_name": "Relation_Owner_OriginatingObjectType_text", "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "OriginatingObjectType_text", "required": True, "conditional": "", "tc_code": "", "description": "Owner OriginatingObjectType text",              "max_length": 20, "regex": "", "allowed_values": ["Holding"]},
    {"column_name": "Relation_Owner_RelatedObjectType_tc",      "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "RelatedObjectType",     "required": True,  "conditional": "",  "tc_code": "6", "description": "Owner RelatedObjectType tc – 6 (Party)",        "max_length": 5,  "regex": "", "allowed_values": ["6"]},
    {"column_name": "Relation_Owner_RelatedObjectType_text",    "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "RelatedObjectType_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Owner RelatedObjectType text",                  "max_length": 20, "regex": "", "allowed_values": ["Party"]},
    {"column_name": "Relation_Owner_RoleCode_tc",               "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "RelationRoleCode",      "required": True,  "conditional": "",  "tc_code": "8", "description": "Owner RelationRoleCode tc – 8 (Owner)",         "max_length": 5,  "regex": "", "allowed_values": ["8"]},
    {"column_name": "Relation_Owner_RoleCode_text",             "spark_type": "STRING", "group": GROUP_REL_OWNER, "xml_tag": "RelationRoleCode_text", "required": True,  "conditional": "",  "tc_code": "",  "description": "Owner RelationRoleCode text",                    "max_length": 30, "regex": "", "allowed_values": ["Owner"]},

    # ── Relation – Annuitant ────────────────────────────────────────────
    {"column_name": "Relation_Annuitant_id",                        "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "Relation",                "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "",  "description": "Annuitant Relation id attribute",                "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Annuitant_OriginatingObjectID",       "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "OriginatingObjectID",   "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "",  "description": "Annuitant Relation OriginatingObjectID",          "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Annuitant_RelatedObjectID",           "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "RelatedObjectID",       "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "",  "description": "Annuitant Relation RelatedObjectID",              "max_length": 50, "regex": "", "allowed_values": []},
    {"column_name": "Relation_Annuitant_OriginatingObjectType_tc",  "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "OriginatingObjectType", "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "4", "description": "Annuitant OriginatingObjectType tc – 4 (Holding)", "max_length": 5, "regex": "", "allowed_values": ["4"]},
    {"column_name": "Relation_Annuitant_OriginatingObjectType_text", "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "OriginatingObjectType_text", "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "", "description": "Annuitant OriginatingObjectType text",           "max_length": 20, "regex": "", "allowed_values": ["Holding"]},
    {"column_name": "Relation_Annuitant_RelatedObjectType_tc",      "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "RelatedObjectType",     "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "6", "description": "Annuitant RelatedObjectType tc – 6 (Party)",      "max_length": 5, "regex": "", "allowed_values": ["6"]},
    {"column_name": "Relation_Annuitant_RelatedObjectType_text",    "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "RelatedObjectType_text", "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "",  "description": "Annuitant RelatedObjectType text",                "max_length": 20, "regex": "", "allowed_values": ["Party"]},
    {"column_name": "Relation_Annuitant_RoleCode_tc",               "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "RelationRoleCode",      "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "35", "description": "Annuitant RelationRoleCode tc – 35 (Annuitant)",    "max_length": 5, "regex": "", "allowed_values": ["35"]},
    {"column_name": "Relation_Annuitant_RoleCode_text",             "spark_type": "STRING", "group": GROUP_REL_ANNUITANT, "xml_tag": "RelationRoleCode_text", "required": False, "conditional": "Required when Annuitant Party present", "tc_code": "",  "description": "Annuitant RelationRoleCode text",                  "max_length": 30, "regex": "", "allowed_values": ["Annuitant"]},

    # ── Test metadata ───────────────────────────────────────────────────
    {"column_name": "ExpectedToPass",  "spark_type": "STRING", "group": GROUP_META, "xml_tag": "",  "required": False, "conditional": "",  "tc_code": "", "description": "Test expectation flag (1=pass, 0=fail)", "max_length": 1, "regex": "^[01]$", "allowed_values": ["0", "1"]},
    {"column_name": "FailureReason",   "spark_type": "STRING", "group": GROUP_META, "xml_tag": "",  "required": False, "conditional": "",  "tc_code": "", "description": "Expected failure reason for test rows",  "max_length": 0, "regex": "", "allowed_values": []},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_spark_schema() -> StructType:
    """Return a PySpark StructType built from FIELD_DEFINITIONS (all STRING, all nullable)."""
    return StructType([
        StructField(fd["column_name"], StringType(), True)
        for fd in FIELD_DEFINITIONS
    ])


def get_column_names() -> list[str]:
    """Return ordered list of column names."""
    return [fd["column_name"] for fd in FIELD_DEFINITIONS]


def get_required_fields() -> list[dict]:
    """Return field definitions where required=True."""
    return [fd for fd in FIELD_DEFINITIONS if fd["required"]]


def get_fields_by_group(group: str) -> list[dict]:
    """Return field definitions belonging to the given group."""
    return [fd for fd in FIELD_DEFINITIONS if fd["group"] == group]


def get_field_by_column(column_name: str) -> dict | None:
    """Look up a single field definition by column name."""
    for fd in FIELD_DEFINITIONS:
        if fd["column_name"] == column_name:
            return fd
    return None


def get_data_fields() -> list[dict]:
    """Return field definitions excluding test-metadata columns."""
    return [fd for fd in FIELD_DEFINITIONS if fd["group"] != GROUP_META]
