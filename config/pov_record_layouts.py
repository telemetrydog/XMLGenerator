""" 
DTCC Positions & Valuations (POV) fixed-width record layout definitions.

Covers all record types in the POV / PFF / PVF / PNF file formats and
the FAR (Financial Activity Reporting) record types that may appear in
the same transmission.

Each record type maps to an *ordered* list of (field_name, width) tuples.
Order matters – fields are parsed left-to-right by byte position.

Source: DTCC I&RS POV Record Layouts (public field names; byte widths
derived from the canonical spacing definitions).
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Dict, List, Tuple

FieldSpec = List[Tuple[str, int]]
RecordLayoutMap = Dict[str, FieldSpec]

# ── POV header records ────────────────────────────────────────────────
_R100: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Submitting_Participant_Number", 4),
    ("IPS_Business_Code", 3),
    ("Transmission_Unique_ID", 30),
    ("Total_Count", 12),
    ("Valuation_Date", 8),
    ("Test_Indicator", 1),
    ("Associated_Carrier_Company_ID", 10),
    ("Filler", 217),
    ("Reject_Code", 12),
]

_R120: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Contra_Participant_Number", 4),
    ("Associated_Firm_ID", 4),
    ("Associated_Firm_Submitted_Contract_Count", 10),
    ("Associated_Firm_Delivered_Contract_Count", 10),
    ("IPS_Event_Code", 3),
    ("IPS_Stage_Code", 3),
    ("Filler", 251),
    ("Reject_Code", 12),
]

# ── POV detail records (13xx) ────────────────────────────────────────
_R1301: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("CUSIP_Number", 9),
    ("Contract_Status", 2),
    ("End_Receiving_Company_ID", 20),
    ("End_Receiving_Company_ID_Qualifier", 2),
    ("Group_Number", 30),
    ("Original_Contract_Number", 30),
    ("Distributors_Account_ID", 30),
    ("IRS_Qualification_Code", 4),
    ("Product_Type_Code", 3),
    ("Commission_Option", 4),
    ("Filler", 6),
    ("Commission_Extension", 10),
    ("ERISA_Indicator", 1),
    ("Contract_State", 2),
    ("Fund_Transfers_Restriction_Indicator", 1),
    ("Fund_Transfers_Restriction_Reason", 2),
    ("Non_Assignibility_Indicator", 1),
    ("Life_Term_Duration", 2),
    ("Filler_2", 94),
    ("Reject_Code", 12),
]

_R1302: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Contract_Value_Amount_1", 16),
    ("Contract_Value_Qualifier_1", 3),
    ("Filler", 1),
    ("Contract_Value_Amount_2", 16),
    ("Contract_Value_Qualifier_2", 3),
    ("Filler_2", 1),
    ("Contract_Value_Amount_3", 16),
    ("Contract_Value_Qualifier_3", 3),
    ("Filler_3", 1),
    ("Contract_Value_Amount_4", 16),
    ("Contract_Value_Qualifier_4", 3),
    ("Filler_4", 1),
    ("Contract_Value_Amount_5", 16),
    ("Contract_Value_Qualifier_5", 3),
    ("Filler_5", 1),
    ("Contract_Percentage_Amount_1", 10),
    ("Contract_Percentage_Qualifier_1", 3),
    ("Filler_6", 1),
    ("Contract_Percentage_Amount_2", 10),
    ("Contract_Percentage_Qualifier_2", 3),
    ("Filler_7", 1),
    ("Contract_Percentage_Amount_3", 10),
    ("Contract_Percentage_Qualifier_3", 3),
    ("Filler_8", 112),
    ("Reject_Code", 12),
]

_R1303: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("CUSIP_Fund_ID_Sub_Fund_ID", 19),
    ("Fund_Value", 16),
    ("Fund_Percentage", 10),
    ("Fund_Units", 18),
    ("Fund_Guaranteed_Interest_Rate", 10),
    ("Fund_Underlying_Security_Name", 40),
    ("Fund_Underlying_Security_Type", 3),
    ("Mutual_Fund_CUSIP_Number", 9),
    ("Fund_Level_Restriction_Indicator", 1),
    ("Fund_Level_Restriction_Reason", 1),
    ("Standing_Allocation_Indicator", 1),
    ("Standing_Allocation_Percentage", 10),
    ("Maturity_Election_Instructions", 2),
    ("Filler", 113),
    ("Reject_Code", 12),
]

_R1304: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("CUSIP_Fund_ID", 19),
    ("Deposit_Guaranteed_Start_Date", 8),
    ("Deposit_Guaranteed_End_Date", 8),
    ("Deposit_Guaranteed_Maturity_Date", 8),
    ("Deposit_Guaranteed_Rate_1", 10),
    ("Deposit_Guaranteed_Rate_Type_1", 2),
    ("Deposit_Guaranteed_Units", 18),
    ("Deposit_Guaranteed_Period_Frequency_Code", 2),
    ("Deposit_Guaranteed_Period_Number", 10),
    ("Deposit_Guarantee_Value", 16),
    ("Deposit_Guaranteed_Rate_2", 10),
    ("Deposit_Guaranteed_Rate_Type_2", 2),
    ("Deposit_Guaranteed_Rate_3", 10),
    ("Deposit_Guaranteed_Rate_Type_3", 2),
    ("Deposit_Guaranteed_Rate_4", 10),
    ("Deposit_Guaranteed_Rate_Type_4", 2),
    ("Deposit_Guaranteed_Rate_5", 10),
    ("Deposit_Guaranteed_Rate_Type_5", 2),
    ("Deposit_Guaranteed_Rate_6", 10),
    ("Deposit_Guaranteed_Rate_Type_6", 2),
    ("Filler", 92),
    ("Reject_Code", 12),
]

_R1305: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Agent_Tax_ID", 20),
    ("Agent_Tax_ID_Qualifier", 2),
    ("Agent_Role", 2),
    ("Agent_Non_Natural_Name", 105),
    ("Agent_Last_Name", 35),
    ("Agent_First_Name", 25),
    ("Agent_Middle_Name", 25),
    ("Agent_Prefix", 10),
    ("Agent_Suffix", 10),
    ("Brokers_Agent_ID", 20),
    ("Agent_Natural_Non_Natural_Name_Indicator", 1),
    ("National_Producer_Number", 10),
    ("Fund_Transfer_Agent_Authorization_Indicator", 1),
    ("Filler", 92),
    ("Reject_Code", 12),
]

_R1306: FieldSpec = []
_1306_date_slots = 20
for _i in range(1, _1306_date_slots + 1):
    if _i == 1:
        _R1306 = [
            ("Submitters_Code", 1),
            ("Record_Type", 2),
            ("Sequence_Number", 2),
            ("Contract_Number", 30),
        ]
    _R1306.append((f"Contract_Date_{_i}", 8))
    _R1306.append((f"Contract_Date_Qualifier_{_i}", 3))
    if _i < _1306_date_slots:
        _R1306.append((f"Filler_{_i}", 1))
_R1306.append(("Filler_20", 14))
_R1306.append(("Reject_Code", 12))

_R1307: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
]
for _i in range(1, 6):
    _R1307.extend([
        (f"Event_Period_Type_{_i}", 3),
        (f"Event_Total_Amount_{_i}", 16),
        (f"Event_Type_Code_{_i}", 3),
        (f"Gross_Net_Indicator_{_i}", 1),
    ])
    if _i < 5:
        _R1307.append((f"Filler_{_i}", 1))
for _i in range(1, 6):
    _R1307.append((f"Next_Event_Date_{_i}", 8))
_R1307.append(("Filler_5", 94))
_R1307.append(("Reject_Code", 12))

_R1309: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Party_Non_Natural_Entity_Name", 105),
    ("Party_Last_Name", 35),
    ("Party_First_Name", 25),
    ("Party_Middle_Name", 25),
    ("Party_Prefix", 10),
    ("Party_Suffix", 10),
    ("Party_Role", 2),
    ("Party_ID", 20),
    ("Party_ID_Qualifier", 2),
    ("Party_Date_of_Birth", 8),
    ("Party_Non_Natural_Entity_Date", 8),
    ("Party_Non_Natural_Entity_Date_Qualifier", 3),
    ("Party_Natural_Non_Natural_Entity_Name_Indicator", 1),
    ("Contract_Party_Role_Qualifier", 1),
    ("Impaired_Risk", 1),
    ("Trust_Revocability_Indicator", 1),
    ("Party_Gender", 1),
    ("Beneficiary_Amount_Quantity", 16),
    ("Beneficiary_Quantity_Qualifier", 2),
    ("Beneficiary_Quantity_Percent", 10),
    ("Filler", 80),
    ("Reject_Code", 12),
]

_R1310: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Party_Role", 2),
    ("Party_Address_Line_1", 35),
    ("Party_Address_Line_2", 35),
    ("Party_City", 30),
    ("Party_State", 2),
    ("Party_Postal_Code", 15),
    ("Party_Country_Code", 3),
    ("Party_Address_Line_3", 35),
    ("Party_Address_Line_4", 35),
    ("Party_Address_Line_5", 35),
    ("Foreign_Address_Indicator", 1),
    ("Filler", 25),
    ("Reject_Code", 12),
]

_R1311: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Annuity_Payout_Amount", 16),
    ("Annuity_Payment_Amount_Qualifier", 3),
    ("Annuity_Frequency_Code", 3),
    ("Annuity_Payout_Type_Code", 3),
    ("Annuity_Certain_Period", 3),
    ("Annuity_Certain_Period_Qualifier", 1),
    ("Annuity_COLA_Percentage", 10),
    ("Annuity_COLA_Type", 2),
    ("Annuity_Commencement_Date", 8),
    ("Number_of_Payments_Remaining", 5),
    ("Annuity_Payout_End_Date", 8),
    ("Number_of_Annuitants", 2),
    ("Annuity_Payment_Status_Indicator", 1),
    ("First_Annuitant_Date_of_Birth", 8),
    ("First_Annuitant_Gender", 1),
    ("Second_Annuitant_Date_of_Birth", 8),
    ("Second_Annuitant_Gender", 1),
    ("Annuity_Joint_Survivor_Percentage", 10),
    ("Annual_Income_Amount", 16),
    ("Annuity_Exclusion_Ratio_Amount", 16),
    ("Annuity_Exclusion_Ratio_Percentage", 10),
    ("Annuity_Cost_Basis_Amount", 16),
    ("Annuity_Payout_Enhancement_Amount", 16),
    ("Annuity_Payout_Enhancement_Percentage", 10),
    ("Annuity_Payout_Enhancement_Period", 3),
    ("Annuity_Payout_Enhancement_Period_Qualifier", 1),
    ("Annuity_Source_Amount_Qualifier", 2),
    ("Annuity_Source_Account_Value", 16),
    ("Filler", 54),
    ("Reject_Code", 12),
]

_R1315: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Filler", 39),
    ("Service_Feature_Value", 14),
    ("Service_Feature_Value_Qualifier", 2),
    ("Service_Feature_Frequency", 1),
    ("Filler_2", 1),
    ("Service_Feature_Start_Date", 8),
    ("Service_Feature_Stop_Date", 8),
    ("Service_Feature_Expense_Type", 2),
    ("Service_Feature_Expense_Dollar_Amount", 14),
    ("Service_Feature_Expense_Percentage", 10),
    ("Service_Feature_Name", 35),
    ("Service_Feature_Product_Code", 20),
    ("Service_Feature_Program_Type", 1),
    ("Service_Feature_Type_Code_1", 4),
    ("Service_Feature_Sub_Type_Code_1", 4),
    ("Service_Feature_Type_Code_2", 4),
    ("Service_Feature_Sub_Type_Code_2", 4),
    ("Service_Feature_Type_Code_3", 4),
    ("Service_Feature_Sub_Type_Code_3", 4),
    ("Service_Feature_Type_Code_4", 4),
    ("Service_Feature_Sub_Type_Code_4", 4),
    ("Service_Feature_Type_Code_5", 4),
    ("Service_Feature_Sub_Type_Code_5", 4),
    ("Service_Feature_Type_Code_6", 4),
    ("Service_Feature_Sub_Type_Code_6", 4),
    ("Filler_3", 50),
    ("Reject_Code", 12),
]


_R1314: FieldSpec = [
    ("Submitters_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("CUSIP_Fund_ID", 9),
    ("Guaranteed_Benefit_Type", 3),
    ("Guaranteed_Benefit_Basis", 2),
    ("Guaranteed_Benefit_Value", 16),
    ("Guaranteed_Benefit_Effective_Date", 8),
    ("Guaranteed_Benefit_Expiration_Date", 8),
    ("Guaranteed_Benefit_Reset_Date", 8),
    ("Guaranteed_Benefit_Step_Up_Type", 1),
    ("Guaranteed_Benefit_Step_Up_Frequency", 2),
    ("Guaranteed_Benefit_Roll_Up_Rate", 10),
    ("Guaranteed_Benefit_Charge_Type", 1),
    ("Guaranteed_Benefit_Charge_Amount", 16),
    ("Guaranteed_Benefit_Charge_Percentage", 5),
    ("Guaranteed_Benefit_Status", 2),
    ("Guaranteed_Benefit_Election_Date", 8),
    ("Guaranteed_Benefit_Base_Amount", 16),
    ("Guaranteed_Benefit_Base_Amount_Qualifier", 2),
    ("Guaranteed_Benefit_Rider_Charge_Basis", 2),
    ("Guaranteed_Benefit_Rider_Charge_Amount", 16),
    ("Filler", 40),
    ("Guaranteed_Benefit_Waiting_Period_End_Date", 8),
    ("Guaranteed_Benefit_Highest_Anniversary_Value", 16),
    ("Guaranteed_Benefit_HAV_Qualifier", 2),
    ("Filler_2", 28),
    ("Guaranteed_Benefit_Next_Step_Up_Date", 8),
    ("Guaranteed_Benefit_Payout_Date", 8),
    ("Guaranteed_Benefit_Payout_Frequency", 4),
    ("Filler_3", 4),
    ("Reject_Code", 12),
]

# ── FAR header records ───────────────────────────────────────────────
_R400: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Transmission_Unique_ID", 30),
    ("Transmission_Date", 8),
    ("Transmitting_Company_Identifier", 4),
    ("Filler", 41),
    ("Settling_Carrier_Identifier", 4),
    ("Filler_2", 35),
    ("Associated_Carrier_Company_Identifier", 10),
    ("Filler_3", 35),
    ("IPS_Business_Event_Code", 3),
    ("Total_Count", 12),
    ("Test_Indicator", 1),
    ("Filler_4", 102),
    ("Reject_Code", 12),
]

_R420: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Contra_Participant_Number", 4),
    ("Associated_Firm_ID", 4),
    ("Associated_Firm_Submitted_Contract_Count", 10),
    ("Associated_Firm_Delivered_Contract_Count", 10),
    ("Stage_Code", 3),
    ("Filler", 254),
    ("Analytic_Reporting_Indicator", 1),
    ("Analytic_Reporting_Firm_Tax_Id", 9),
    ("Analytic_Reporting_Firm_Name", 70),
    ("Filler_2", 174),
    ("Reject_Code", 12),
]

# ── FAR detail records (43xx) ────────────────────────────────────────
_R4301: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Group_Number", 30),
    ("IRS_Qualification_Code", 4),
    ("Distributor_Account_Number", 30),
    ("CUSIP_Number", 9),
    ("Contract_Status", 2),
    ("Contract_Date_1", 8),
    ("Contract_Date_Qualifier_1", 3),
    ("Contract_Date_2", 8),
    ("Contract_Date_Qualifier_2", 3),
    ("Contract_Date_3", 8),
    ("Contract_Date_Qualifier_3", 3),
    ("Contract_Date_4", 8),
    ("Contract_Date_Qualifier_4", 3),
    ("Contract_Date_5", 8),
    ("Contract_Date_Qualifier_5", 3),
    ("End_Receiving_Company_ID", 20),
    ("End_Receiving_Company", 2),
    ("Filler", 1),
    ("Commission_Option", 4),
    ("Filler_2", 6),
    ("Product_Type_Code", 3),
    ("Filler_3", 87),
    ("Reject_Code", 12),
]

_R4302: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Contract_Entity_Type_Code", 1),
    ("Contract_Entity_Role", 3),
    ("Contract_Entity_Natural_Non_Natural_Name_Indicator", 1),
    ("Non_Natural_Entity_Name", 105),
    ("Contract_Entity_Last_Name", 35),
    ("Contract_Entity_First_Name", 25),
    ("Contract_Entity_Middle_Name", 25),
    ("Contract_Entity_Prefix", 10),
    ("Contract_Entity_Suffix", 10),
    ("Filler", 75),
    ("Contract_Entity_Personal_Identifier", 20),
    ("Contract_Entity_Personal_Qualifier", 2),
    ("Trust_Revocability_Indicator", 1),
    ("Filler_2", 45),
    ("Reject_Code", 12),
]

_R4303: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Contract_Entity_Address_Line_1", 35),
    ("Contract_Entity_Address_Line_2", 35),
    ("Contract_Entity_Address_Line_3", 35),
    ("Contract_Entity_City", 30),
    ("Contract_Entity_State", 2),
    ("Contract_Entity_Zip", 15),
    ("Contract_Entity_Residence_Country", 3),
    ("Contract_Entity_Address_Line_4", 35),
    ("Contract_Entity_Address_Line_5", 35),
    ("Foreign_Address_Indicator", 1),
    ("Filler", 27),
    ("Reject_Code", 12),
]

_R4304: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Agent_Tax_ID", 20),
    ("Agent_Tax_ID_Qualifier", 2),
    ("Agent_Non_Natural_Name", 105),
    ("Agent_Last_Name", 35),
    ("Agent_First_Name", 25),
    ("Agent_Middle_Name", 25),
    ("Agent_Prefix", 10),
    ("Agent_Suffix", 10),
    ("Agent_Role", 3),
    ("Agent_Type_Code", 1),
    ("Brokers_Agent_ID", 20),
    ("Agent_Natural_Non_Natural_Name_Indicator", 1),
    ("National_Producer_Number", 10),
    ("Filler", 91),
    ("Reject_Code", 12),
]

_R4305: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("NSCC_Control_Number", 20),
    ("Distributor_Transaction_Identifier", 30),
    ("Transaction_Amount", 16),
    ("Transaction_Amount_Debit_Credit_Indicator", 1),
    ("Transaction_Source_Indicator", 1),
    ("Transaction_Identifier", 3),
    ("Transaction_Charges_Benefits_1", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_1", 1),
    ("Transaction_Charges_Benefits_Qualifier_1", 3),
    ("Transaction_Charges_Benefits_2", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_2", 1),
    ("Transaction_Charges_Benefits_Qualifier_2", 3),
    ("Transaction_Charges_Benefits_3", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_3", 1),
    ("Transaction_Charges_Benefits_Qualifier_3", 3),
    ("Transaction_Charges_Benefits_4", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_4", 1),
    ("Transaction_Charges_Benefits_Qualifier_4", 3),
    ("Transaction_Charges_Benefits_5", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_5", 1),
    ("Transaction_Charges_Benefits_Qualifier_5", 3),
    ("Transaction_Charges_Benefits_6", 16),
    ("Transaction_Charges_Benefits_DC_Indicator_6", 1),
    ("Transaction_Charges_Benefits_Qualifier_6", 3),
    ("Application_Control_Number", 20),
    ("Payee_Payor_Payment_Method", 3),
    ("Payment_Type", 2),
    ("Filler", 15),
    ("Transaction_Date_Effective", 8),
    ("Transaction_Date_Process", 8),
    ("Tax_Year", 4),
    ("Filler_2", 2),
    ("Reject_Code", 12),
]

_R4306: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Fund_Identifier", 19),
    ("Filler", 1),
    ("Fund_Amount", 16),
    ("Fund_Amount_Debit_Credit_Indicator", 1),
    ("MVA_Amount", 16),
    ("MVA_Amount_Debit_Credit_Indicator", 1),
    ("Employer_Amount", 16),
    ("Employer_Amount_Debit_Credit_Indicator", 1),
    ("Employee_Amount", 16),
    ("Employee_Amount_Debit_Credit_Indicator", 1),
    ("Fund_Surrender_Charges", 16),
    ("Fund_Surrender_Debit_Credit_Indicator", 1),
    ("Fund_Administrative_Charges", 16),
    ("Fund_Administrative_Debit_Credit_Indicator", 1),
    ("Fund_Unit_Price", 15),
    ("Filler_2", 2),
    ("Fund_Units", 18),
    ("Fund_Units_Debit_Credit_Indicator", 1),
    ("Mutual_Fund_CUSIP_Number", 9),
    ("Filler_3", 1),
    ("Deposit_Period_Start_Date", 8),
    ("Deposit_Period_End_Date", 8),
    ("Deposit_Period_Maturity_Date", 8),
    ("Deposit_Period_Rate", 10),
    ("Deposit_Period_Rate_Type", 2),
    ("Deposit_Period_Duration", 10),
    ("Deposit_Period_Duration_Qualifier", 2),
    ("Employer_Amount_Identifier_Qualifier", 4),
    ("Employee_Amount_Identifier_Qualifier", 4),
    ("Filler_4", 29),
    ("Reject_Code", 12),
]

_R4307: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Payee_Payor_Entity_Type_Code", 1),
    ("Payee_Payor_Entity_Role", 3),
    ("Payee_Payor_Natural_Non_Natural_Indicator", 1),
    ("Payee_Payor_Non_Natural_Entity_Name", 105),
    ("Payee_Payor_Entity_Last_Name", 35),
    ("Payee_Payor_Entity_First_Name", 25),
    ("Payee_Payor_Entity_Middle_Name", 25),
    ("Payee_Payor_Entity_Prefix", 10),
    ("Payee_Payor_Entity_Suffix", 10),
    ("Filler", 75),
    ("Payee_Payor_Entity_Personal_Identifier", 20),
    ("Payee_Payor_Entity_Identifier_Qualifier", 2),
    ("Filler_2", 46),
    ("Reject_Code", 12),
]

_R4308: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Filler", 3),
    ("Payee_Payor_Payment_Method_Net_Amount", 16),
    ("Payee_Payor_Payment_Net_DC_Indicator", 1),
    ("Payment_Federal_Tax_Amount_Withheld", 16),
    ("Payment_Federal_Tax_DC_Indicator", 1),
    ("Filler_2", 4),
    ("Payor_Account_Number", 20),
    ("Payor_Account_Type_Destination_Identifier", 3),
    ("Payee_Payor_Additional_Account_Number", 20),
    ("Payee_Payor_Additional_Account_Type", 3),
    ("Payment_State_Tax_Amount_Withheld", 16),
    ("Payment_State_Tax_DC_Indicator", 1),
    ("Payment_Source_Amount_Qualifier", 2),
    ("Filler_3", 147),
    ("Reject_Code", 12),
]

_R4309: FieldSpec = [
    ("System_Code", 1),
    ("Record_Type", 2),
    ("Sequence_Number", 2),
    ("Contract_Number", 30),
    ("Payee_Payor_Entity_Address_Line_1", 35),
    ("Payee_Payor_Entity_Address_Line_2", 35),
    ("Payee_Payor_Entity_Address_Line_3", 35),
    ("Payee_Payor_Entity_City", 30),
    ("Payee_Payor_Entity_State", 2),
    ("Payee_Payor_Entity_Zip", 15),
    ("Payee_Payor_Entity_Country", 3),
    ("Payee_Payor_Entity_Address_Line_4", 35),
    ("Payee_Payor_Entity_Address_Line_5", 35),
    ("Foreign_Address_Indicator", 1),
    ("Filler", 27),
    ("Reject_Code", 12),
]

# ── Master layout registry ───────────────────────────────────────────
RECORD_LAYOUTS: RecordLayoutMap = OrderedDict([
    ("100",  _R100),
    ("120",  _R120),
    ("1301", _R1301),
    ("1302", _R1302),
    ("1303", _R1303),
    ("1304", _R1304),
    ("1305", _R1305),
    ("1306", _R1306),
    ("1307", _R1307),
    ("1309", _R1309),
    ("1310", _R1310),
    ("1311", _R1311),
    ("1314", _R1314),
    ("1315", _R1315),
    ("400",  _R400),
    ("420",  _R420),
    ("4301", _R4301),
    ("4302", _R4302),
    ("4303", _R4303),
    ("4304", _R4304),
    ("4305", _R4305),
    ("4306", _R4306),
    ("4307", _R4307),
    ("4308", _R4308),
    ("4309", _R4309),
])

POV_DETAIL_TYPES = {"1301", "1302", "1303", "1304", "1305", "1306",
                    "1307", "1309", "1310", "1311", "1314", "1315"}
POV_HEADER_TYPES = {"100", "120"}
FAR_DETAIL_TYPES = {"4301", "4302", "4303", "4304", "4305", "4306",
                    "4307", "4308", "4309"}
FAR_HEADER_TYPES = {"400", "420"}

# Repeating record types – a single contract can have multiple of these
REPEATING_RECORD_TYPES = {
    "1302", "1303", "1304", "1305", "1306", "1307", "1309", "1310",
    "1311", "1314", "1315",
    "4302", "4303", "4304", "4305", "4306", "4307", "4308", "4309",
}

# Record type descriptions (for scorecard / reporting)
RECORD_TYPE_DESCRIPTIONS = {
    "100":  "File Header",
    "120":  "Firm Header",
    "1301": "Contract Header",
    "1302": "Contract Values",
    "1303": "Fund Detail",
    "1304": "Deposit/Guaranteed Rate Detail",
    "1305": "Agent Information",
    "1306": "Contract Dates",
    "1307": "Events",
    "1309": "Party Information",
    "1310": "Party Address",
    "1311": "Annuity Payout",
    "1314": "Guaranteed Minimum Benefits",
    "1315": "Service Features",
    "400":  "FAR File Header",
    "420":  "FAR Firm Header",
    "4301": "FAR Contract Header",
    "4302": "FAR Contract Entity",
    "4303": "FAR Contract Entity Address",
    "4304": "FAR Agent",
    "4305": "FAR Transaction Detail",
    "4306": "FAR Fund Detail",
    "4307": "FAR Payee/Payor Entity",
    "4308": "FAR Payment Detail",
    "4309": "FAR Payee/Payor Address",
}


def get_layout(record_type: str) -> FieldSpec | None:
    """Return the field spec for a given record type, or None."""
    return RECORD_LAYOUTS.get(record_type)


def get_field_names(record_type: str, *, include_filler: bool = False) -> list[str]:
    """Return field names for a record type, optionally excluding fillers."""
    layout = RECORD_LAYOUTS.get(record_type, [])
    if include_filler:
        return [name for name, _ in layout]
    return [name for name, _ in layout if not name.startswith("Filler")]


def get_record_width(record_type: str) -> int:
    """Total byte width for a record type."""
    layout = RECORD_LAYOUTS.get(record_type, [])
    return sum(w for _, w in layout)


# Alias matching the requirements document naming convention.
get_total_width = get_record_width


# ── File format constants ─────────────────────────────────────────────

FORMAT_STANDARD = "standard"    # Original: HDR/END text lines, 100/120 headers, 300-char lines
FORMAT_EXTENDED = "extended"    # Production: no headers, 336-char padded lines, 36-byte extension

STANDARD_LINE_WIDTH = 300
EXTENDED_LINE_WIDTH = 336

# 36-byte transmission extension appended to records with <=300 layout width
# Only present in the extended format.  Ignored by default parsing.
_EXTENSION_36: FieldSpec = [
    ("Ext_Carrier_Routing_Code", 4),
    ("Ext_File_Format_Type", 3),
    ("Ext_Contra_Participant", 4),
    ("Ext_Firm_Abbreviation", 3),
    ("Ext_Contract_Ref", 20),
    ("Ext_Record_Sequence", 2),
]


def detect_file_format(filepath: str, *, sample_lines: int = 20) -> str:
    """
    Detect whether a POV file is standard or extended format.

    Standard format:
        - Starts with an HDR text line or has 100/120 header records
        - Lines are 300 characters wide
        - Has HDR / END wrapper lines

    Extended format:
        - No header/trailer lines — only detail records
        - Every line is 336 characters wide
        - 36-byte transmission extension on records with <=300-char layouts

    Returns ``FORMAT_STANDARD`` or ``FORMAT_EXTENDED``.
    """
    import os
    if not os.path.isfile(filepath):
        return FORMAT_STANDARD  # default for missing files

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for _ in range(sample_lines):
            raw = fh.readline()
            if not raw:
                break
            line = raw.rstrip("\n\r")
            if not line.strip():
                continue

            # HDR text line → standard format
            if line.startswith("HDR"):
                return FORMAT_STANDARD

            # 100 / 120 header record → standard format
            if len(line) >= 3:
                rt2 = line[1:3]
                if rt2 in ("10", "12"):
                    return FORMAT_STANDARD

            # All-detail-records + 336-char width → extended
            if len(line) == EXTENDED_LINE_WIDTH:
                return FORMAT_EXTENDED

            # Detail record + 300-char width → standard
            if len(line) == STANDARD_LINE_WIDTH:
                return FORMAT_STANDARD

    return FORMAT_STANDARD


def extract_valuation_date_from_filename(filepath: str) -> str:
    """
    Attempt to extract a valuation/file date from the filename.

    Recognises patterns like ``20260320``, ``03202026``, and
    ``YYYYMMDD`` embedded in the basename.  Returns an 8-digit date
    string (``YYYYMMDD``) or empty string if not found.
    """
    import os, re
    basename = os.path.basename(filepath)

    # Pattern 1: YYYYMMDD (e.g., 20260320)
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])", basename)
    if m:
        return m.group(0)

    # Pattern 2: MMDDYYYY (e.g., 03202026)
    m = re.search(r"(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(20\d{2})", basename)
    if m:
        mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}{mm}{dd}"

    return ""


def detect_record_type(line: str) -> str | None:
    """
    Detect the record type code from a fixed-width line.

    POV lines: pos 1-2 = '10'/'12'/'13', with pos 3-4 as sub-type for detail.
    FAR lines: pos 1-2 = '40'/'42'/'43', with pos 3-4 as sub-type for detail.
    """
    if len(line) < 5:
        return None

    rt2 = line[1:3]

    if rt2 == "10":
        return "100"
    if rt2 == "12":
        return "120"
    if rt2 == "13":
        seq = line[3:5]
        code = "13" + seq
        return code if code in RECORD_LAYOUTS else None
    if rt2 == "40":
        return "400"
    if rt2 == "42":
        return "420"
    if rt2 == "43":
        seq = line[3:5]
        code = "43" + seq
        return code if code in RECORD_LAYOUTS else None

    return None
