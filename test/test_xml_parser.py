import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from lxml import etree

from app.xml_parser import XMLParser

NS = "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"

SAMPLE_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="{NS}">
  <FinInstrm>
    <TermntdRcrd>
      <FinInstrmGnlAttrbts>
        <Id>ID001</Id>
        <FullNm>Apple Inc</FullNm>
        <ClssfctnTp>ESVUFR</ClssfctnTp>
        <CmmdtyDerivInd>false</CmmdtyDerivInd>
        <NtnlCcy>USD</NtnlCcy>
      </FinInstrmGnlAttrbts>
      <Issr>ISSUER001</Issr>
    </TermntdRcrd>
    <TermntdRcrd>
      <FinInstrmGnlAttrbts>
        <Id>ID002</Id>
        <FullNm>Bcorp Ltd</FullNm>
        <ClssfctnTp>DBFTFR</ClssfctnTp>
        <CmmdtyDerivInd>true</CmmdtyDerivInd>
        <NtnlCcy>EUR</NtnlCcy>
      </FinInstrmGnlAttrbts>
      <Issr>ISSUER002</Issr>
    </TermntdRcrd>
  </FinInstrm>
</Document>""".encode()


@pytest.fixture
def xml_file(tmp_path):
    path = tmp_path / "test.xml"
    path.write_bytes(SAMPLE_XML)
    return str(path)


@pytest.fixture
def parser(xml_file, tmp_path):
    csv_folder = str(tmp_path / "csv")
    return XMLParser(xml_file, csv_folder_name=csv_folder)


class TestExtractXmlFromFile:
    def test_parses_valid_xml_file(self, parser, tmp_path):
        parser.extract_xml_from_file()

        csv_files = list((tmp_path / "csv").glob("*.csv"))
        assert len(csv_files) == 1

    def test_deletes_xml_file_after_parsing(self, xml_file, tmp_path):
        parser = XMLParser(xml_file, csv_folder_name=str(tmp_path / "csv"))
        assert os.path.exists(xml_file)
        parser.extract_xml_from_file()
        assert not os.path.exists(xml_file)

    def test_raises_for_missing_file(self, tmp_path):
        p = XMLParser("/nonexistent/path/file.xml", csv_folder_name=str(tmp_path / "csv"))
        with pytest.raises(Exception):
            p.extract_xml_from_file()


class TestExtractXmlData:
    def test_extracts_correct_fields(self, parser):
        context = etree.iterparse(
            parser.xml_file_path,
            events=("start", "end"),
            tag=f"{{{NS}}}TermntdRcrd",
        )
        result = parser.extract_xml_data(context)
        # After full flush, returns empty list
        assert result == []

    def test_csv_contains_expected_records(self, parser, tmp_path):
        parser.extract_xml_from_file()

        csv_path = next((tmp_path / "csv").glob("*.csv"))
        df = pd.read_csv(csv_path)

        assert len(df) == 2
        assert df["FinInstrmGnlAttrbts.Id"].tolist() == ["ID001", "ID002"]
        assert df["Issr"].tolist() == ["ISSUER001", "ISSUER002"]

    def test_batching_writes_all_records(self, xml_file, tmp_path):
        csv_folder = str(tmp_path / "csv")
        parser = XMLParser(xml_file, csv_folder_name=csv_folder)
        parser.batch_size = 1  # force a batch flush after every record

        parser.extract_xml_from_file()

        csv_path = next((tmp_path / "csv").glob("*.csv"))
        df = pd.read_csv(csv_path)
        assert len(df) == 2


class TestProcessXmlData:
    def test_a_count_counts_lowercase_a(self, parser):
        data = [{"FinInstrmGnlAttrbts.FullNm": "banana", "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]
        df = parser.process_xml_data(data)
        assert df["a_count"].iloc[0] == 3

    def test_contains_a_is_yes_when_a_present(self, parser):
        data = [{"FinInstrmGnlAttrbts.FullNm": "banana", "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]
        df = parser.process_xml_data(data)
        assert df["contains_a"].iloc[0] == "YES"

    def test_contains_a_is_no_when_no_a(self, parser):
        data = [{"FinInstrmGnlAttrbts.FullNm": "BitCorp", "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]
        df = parser.process_xml_data(data)
        assert df["contains_a"].iloc[0] == "NO"

    def test_handles_none_full_nm(self, parser):
        data = [{"FinInstrmGnlAttrbts.FullNm": None, "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]
        df = parser.process_xml_data(data)
        assert df["a_count"].iloc[0] == 0
        assert df["contains_a"].iloc[0] == "NO"


class TestDumpXmlDataToCsv:
    def test_writes_header_on_first_write(self, parser, tmp_path):
        data = [{"FinInstrmGnlAttrbts.FullNm": "Apple", "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]
        parser.dump_xml_data_to_csv(data)

        csv_path = next((tmp_path / "csv").glob("*.csv"))
        with open(csv_path) as f:
            first_line = f.readline()
        assert "FinInstrmGnlAttrbts.Id" in first_line

    def test_appends_without_duplicate_header(self, parser, tmp_path):
        data = [{"FinInstrmGnlAttrbts.FullNm": "Apple", "FinInstrmGnlAttrbts.Id": "1",
                 "FinInstrmGnlAttrbts.ClssfctnTp": None, "FinInstrmGnlAttrbts.CmmdtyDerivInd": None,
                 "FinInstrmGnlAttrbts.NtnlCcy": None, "Issr": None}]

        parser.dump_xml_data_to_csv(data)
        parser.dump_xml_data_to_csv(data)

        csv_path = next((tmp_path / "csv").glob("*.csv"))
        df = pd.read_csv(csv_path)
        assert len(df) == 2


class TestUploadToCloud:
    def test_calls_fsspec_put_with_correct_paths(self, xml_file, tmp_path):
        local_csv = tmp_path / "test.xml.csv"
        local_csv.write_text("col\nval\n")

        mock_fs = MagicMock()
        stripped = "bucket/csv/test.xml.csv"
        with patch("app.xml_parser.fsspec.core.url_to_fs", return_value=(mock_fs, stripped)) as mock_url_to_fs:
            parser = XMLParser(xml_file, csv_folder_name=str(tmp_path / "csv"), cloud_upload_path="s3://bucket/csv")
            parser._upload_to_cloud(str(local_csv))

        expected_full_path = "s3://bucket/csv/test.xml.csv"
        mock_url_to_fs.assert_called_once_with(expected_full_path)
        mock_fs.put.assert_called_once_with(str(local_csv), stripped)

    def test_raises_on_upload_failure(self, xml_file, tmp_path):
        local_csv = tmp_path / "test.xml.csv"
        local_csv.write_text("col\nval\n")

        mock_fs = MagicMock()
        mock_fs.put.side_effect = Exception("upload failed")
        with patch("app.xml_parser.fsspec.core.url_to_fs", return_value=(mock_fs, "bucket/csv/test.xml.csv")):
            parser = XMLParser(xml_file, csv_folder_name=str(tmp_path / "csv"), cloud_upload_path="s3://bucket/csv")
            with pytest.raises(Exception, match="upload failed"):
                parser._upload_to_cloud(str(local_csv))

    def test_upload_triggered_after_parsing_when_cloud_path_set(self, xml_file, tmp_path):
        mock_fs = MagicMock()
        with patch("app.xml_parser.fsspec.core.url_to_fs", return_value=(mock_fs, "bucket/csv/test.xml.csv")):
            parser = XMLParser(
                xml_file,
                csv_folder_name=str(tmp_path / "csv"),
                cloud_upload_path="s3://bucket/csv",
            )
            parser.extract_xml_from_file()

        assert mock_fs.put.called

    def test_upload_not_triggered_when_no_cloud_path(self, xml_file, tmp_path):
        with patch("app.xml_parser.fsspec.core.url_to_fs") as mock_url_to_fs:
            parser = XMLParser(xml_file, csv_folder_name=str(tmp_path / "csv"))
            parser.extract_xml_from_file()

        mock_url_to_fs.assert_not_called()


class TestExtractDataFromXmlEle:
    def test_returns_text_of_existing_element(self, parser):
        root = etree.Element(f"{{{NS}}}Parent")
        child = etree.SubElement(root, f"{{{NS}}}Child")
        child.text = "hello"
        result = parser.extract_data_from_xml_ele(root, f"{{{NS}}}", "Child")
        assert result == "hello"

    def test_returns_none_for_missing_element(self, parser):
        root = etree.Element(f"{{{NS}}}Parent")
        result = parser.extract_data_from_xml_ele(root, f"{{{NS}}}", "Missing")
        assert result is None
