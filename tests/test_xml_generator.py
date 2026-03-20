"""Tests for XML generator module."""

import os
import sys
import uuid
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.schema_config import ACORD_NAMESPACE, get_spark_schema
from modules.csv_generator import generate_sample_csv
from modules.table_manager import load_csv
from modules.xml_generator import generate_xml_from_row, generate_all_xmls


NS = {"acord": ACORD_NAMESPACE}


def _load_df(spark, tmp_dir, num_valid=3, num_invalid=0):
    csv_path = os.path.join(tmp_dir, "sample.csv")
    generate_sample_csv(csv_path, num_valid=num_valid, num_invalid=num_invalid)
    uid = uuid.uuid4().hex[:8]
    return load_csv(spark, csv_path, f"xmlt_{uid}")


class TestGenerateXmlFromRow:
    def test_well_formed_xml(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        assert root.tag == f"{{{ACORD_NAMESPACE}}}TXLife"

    def test_contains_trans_ref_guid(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        guid = root.find(".//acord:TransRefGUID", NS)
        assert guid is not None
        assert len(guid.text) > 0

    def test_trans_type_212(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        tt = root.find(".//acord:TransType", NS)
        assert tt is not None
        assert tt.get("tc") == "212"

    def test_policy_number_present(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        pol = root.find(".//acord:PolNumber", NS)
        assert pol is not None
        assert pol.text == "ANN-2026-00001"

    def test_holding_type_code_tc_attribute(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        htc = root.find(".//acord:HoldingTypeCode", NS)
        assert htc is not None
        assert htc.get("tc") == "2"

    def test_holding_id_attribute(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        holding = root.find(".//acord:Holding", NS)
        assert holding is not None
        assert holding.get("id") == "Holding_1"

    def test_primary_object_id_attribute(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        root = ET.fromstring(xml_str)
        req = root.find("acord:TXLifeRequest", NS)
        assert req is not None
        assert req.get("PrimaryObjectID") == "Holding_1"

    def test_namespace_correct(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        assert ACORD_NAMESPACE in xml_str

    def test_xml_declaration_present(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        row = df.collect()[0]
        xml_str = generate_xml_from_row(row)
        assert xml_str.startswith("<?xml")


class TestGenerateAllXmls:
    def test_generates_correct_number_of_files(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 3, 0)
        out_dir = os.path.join(tmp_dir, "xml_out")
        results = generate_all_xmls(df, out_dir)
        assert len(results) == 3
        for r in results:
            assert os.path.exists(r["filepath"])

    def test_filenames_contain_policy_number(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 2, 0)
        out_dir = os.path.join(tmp_dir, "xml_out")
        results = generate_all_xmls(df, out_dir)
        for r in results:
            assert r["pol_number"] in r["filename"]

    def test_result_dict_keys(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 1, 0)
        out_dir = os.path.join(tmp_dir, "xml_out")
        results = generate_all_xmls(df, out_dir)
        expected_keys = {"pol_number", "filename", "filepath", "xml_string"}
        assert set(results[0].keys()) == expected_keys

    def test_xml_files_are_parseable(self, spark, tmp_dir):
        df = _load_df(spark, tmp_dir, 3, 0)
        out_dir = os.path.join(tmp_dir, "xml_out")
        results = generate_all_xmls(df, out_dir)
        for r in results:
            with open(r["filepath"]) as f:
                tree = ET.parse(f)
            assert tree.getroot().tag == f"{{{ACORD_NAMESPACE}}}TXLife"
