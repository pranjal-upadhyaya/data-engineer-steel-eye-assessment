"""Integration tests for the full ESMA FIRDS pipeline.

These tests mock only the external HTTP boundary (requests.get) and let all
internal components — XMLFetcher, XMLParser, and XMLExtractorAndParser — run
against real files on the local filesystem using pytest's tmp_path fixture.
"""

import io
import zipfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from app.config import app_config
from app.pipeline import XMLExtractorAndParser
from app.xml_fetcher import XMLFetcher
from app.xml_parser import XMLParser

NS = "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"

# Realistic Solr API response with two DLTINS entries so index 1 is selectable.
METADATA_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<response>
  <result>
    <doc>
      <str name="_root_">root1</str>
      <str name="id">FILE001</str>
      <str name="published_instrument_file_id">FILE001</str>
      <str name="file_name">DLTINS_20210117_01.zip</str>
      <str name="file_type">DLTINS</str>
      <str name="publication_date">2021-01-17T00:00:00Z</str>
      <str name="download_link">https://example.com/DLTINS_20210117_01.zip</str>
      <str name="checksum">checksum1</str>
      <str name="_version_">1111111111</str>
      <str name="timestamp">2021-01-17T00:00:00Z</str>
    </doc>
    <doc>
      <str name="_root_">root2</str>
      <str name="id">FILE002</str>
      <str name="published_instrument_file_id">FILE002</str>
      <str name="file_name">DLTINS_20210117_02.zip</str>
      <str name="file_type">DLTINS</str>
      <str name="publication_date">2021-01-17T00:00:00Z</str>
      <str name="download_link">https://example.com/DLTINS_20210117_02.zip</str>
      <str name="checksum">checksum2</str>
      <str name="_version_">2222222222</str>
      <str name="timestamp">2021-01-17T00:00:00Z</str>
    </doc>
  </result>
</response>"""

DLTINS_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
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
        <FullNm>Banana Corp</FullNm>
        <ClssfctnTp>DBFTFR</ClssfctnTp>
        <CmmdtyDerivInd>true</CmmdtyDerivInd>
        <NtnlCcy>EUR</NtnlCcy>
      </FinInstrmGnlAttrbts>
      <Issr>ISSUER002</Issr>
    </TermntdRcrd>
  </FinInstrm>
</Document>""".encode()


def make_zip(filename: str, content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


def mock_requests_get(metadata_url_fragment: str, zip_bytes: bytes):
    """Return a side_effect function that routes requests by URL."""
    def side_effect(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = (
            METADATA_XML if metadata_url_fragment in url else zip_bytes
        )
        return mock_resp
    return side_effect


class TestFetcherParserIntegration:
    """XMLFetcher and XMLParser working together without the pipeline orchestrator."""

    def test_downloaded_xml_is_parsed_to_csv(self, tmp_path):
        xml_folder = tmp_path / "xml"
        csv_folder = tmp_path / "csv"
        zip_bytes = make_zip("DLTINS_20210117_02.xml", DLTINS_XML)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = zip_bytes

        with patch("app.xml_fetcher.requests.get", return_value=mock_resp):
            fetcher = XMLFetcher("http://test.url", folder_name=str(xml_folder))
            fetcher.download_xml_file("https://example.com/DLTINS_20210117_02.zip")
            xml_files = fetcher.list_downloaded_xml_files()

        assert len(xml_files) == 1

        xml_path = str(xml_folder / xml_files[0])
        parser = XMLParser(xml_path, csv_folder_name=str(csv_folder))
        parser.extract_xml_from_file()

        csv_files = list(csv_folder.glob("*.csv"))
        assert len(csv_files) == 1

        df = pd.read_csv(csv_files[0])
        assert len(df) == 2
        assert list(df.columns) == [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
            "a_count",
            "contains_a",
        ]

    def test_xml_deleted_after_csv_written(self, tmp_path):
        xml_folder = tmp_path / "xml"
        csv_folder = tmp_path / "csv"
        zip_bytes = make_zip("DLTINS_20210117_02.xml", DLTINS_XML)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = zip_bytes

        with patch("app.xml_fetcher.requests.get", return_value=mock_resp):
            fetcher = XMLFetcher("http://test.url", folder_name=str(xml_folder))
            fetcher.download_xml_file("https://example.com/DLTINS_20210117_02.zip")
            xml_files = fetcher.list_downloaded_xml_files()

        xml_path = str(xml_folder / xml_files[0])
        parser = XMLParser(xml_path, csv_folder_name=str(csv_folder))
        parser.extract_xml_from_file()

        assert not (xml_folder / "DLTINS_20210117_02.xml").exists()


class TestPipelineIntegration:
    """Full end-to-end pipeline: metadata fetch → zip download → XML parse → CSV output."""

    def test_produces_csv_with_correct_records(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "base_folder", str(tmp_path))
        zip_bytes = make_zip("DLTINS_20210117_02.xml", DLTINS_XML)

        with patch("app.xml_fetcher.requests.get", side_effect=mock_requests_get("esma", zip_bytes)):
            XMLExtractorAndParser(
                "https://esma.example.com/solr/select",
                xml_folder_name=str(tmp_path / "xml"),
            ).run()

        df = pd.read_csv(next((tmp_path / "csv").glob("*.csv")))
        assert len(df) == 2
        assert df["FinInstrmGnlAttrbts.Id"].tolist() == ["ID001", "ID002"]
        assert df["Issr"].tolist() == ["ISSUER001", "ISSUER002"]

    def test_a_count_and_contains_a_computed_correctly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "base_folder", str(tmp_path))
        zip_bytes = make_zip("DLTINS_20210117_02.xml", DLTINS_XML)

        with patch("app.xml_fetcher.requests.get", side_effect=mock_requests_get("esma", zip_bytes)):
            XMLExtractorAndParser(
                "https://esma.example.com/solr/select",
                xml_folder_name=str(tmp_path / "xml"),
            ).run()

        df = pd.read_csv(next((tmp_path / "csv").glob("*.csv")))

        # "Apple Inc" has no lowercase 'a'; "Banana Corp" has three.
        row1 = df[df["FinInstrmGnlAttrbts.Id"] == "ID001"].iloc[0]
        row2 = df[df["FinInstrmGnlAttrbts.Id"] == "ID002"].iloc[0]
        assert row1["a_count"] == 0 and row1["contains_a"] == "NO"
        assert row2["a_count"] == 3 and row2["contains_a"] == "YES"

    def test_xml_deleted_after_pipeline_completes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "base_folder", str(tmp_path))
        zip_bytes = make_zip("DLTINS_20210117_02.xml", DLTINS_XML)

        with patch("app.xml_fetcher.requests.get", side_effect=mock_requests_get("esma", zip_bytes)):
            XMLExtractorAndParser(
                "https://esma.example.com/solr/select",
                xml_folder_name=str(tmp_path / "xml"),
            ).run()

        assert list((tmp_path / "xml").glob("*.xml")) == []

    def test_raises_on_metadata_fetch_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "base_folder", str(tmp_path))

        with patch("app.xml_fetcher.requests.get", side_effect=requests.exceptions.ConnectionError("unreachable")):
            with pytest.raises(requests.exceptions.ConnectionError):
                XMLExtractorAndParser(
                    "https://esma.example.com/solr/select",
                    xml_folder_name=str(tmp_path / "xml"),
                ).run()

    def test_raises_when_no_dltins_file_at_index(self, tmp_path, monkeypatch):
        monkeypatch.setattr(app_config, "base_folder", str(tmp_path))

        # Metadata with only one DLTINS entry — index 1 does not exist.
        single_entry_xml = METADATA_XML.replace(
            b"<str name=\"_root_\">root2</str>", b""
        ).split(b"<doc>")[0] + b"</result></response>"

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = single_entry_xml

        with patch("app.xml_fetcher.requests.get", return_value=mock_resp):
            with pytest.raises(Exception):
                XMLExtractorAndParser("https://esma.example.com/solr/select").run()
