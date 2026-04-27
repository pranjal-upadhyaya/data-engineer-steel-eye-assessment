import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest
import requests

from app.xml_fetcher import XMLFetcher

SAMPLE_METADATA_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<response>
  <result>
    <doc>
      <str name="_root_">abc123</str>
      <str name="id">FILE001</str>
      <str name="published_instrument_file_id">FILE001</str>
      <str name="file_name">DLTINS_20210117.zip</str>
      <str name="file_type">DLTINS</str>
      <str name="publication_date">2021-01-17T00:00:00Z</str>
      <str name="download_link">https://example.com/DLTINS_20210117.zip</str>
      <str name="checksum">abc123checksum</str>
      <str name="_version_">1234567890</str>
      <str name="timestamp">2021-01-17T00:00:00Z</str>
    </doc>
  </result>
</response>"""


def make_zip_bytes(filename: str = "test.xml", content: bytes = b"<xml/>") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


class TestExtractXmlFileMetadata:
    def test_returns_list_of_models_on_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = SAMPLE_METADATA_XML

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url")
            result = fetcher.extract_xml_file_metadata()

        assert len(result) == 1
        assert result[0].file_type == "DLTINS"
        assert result[0].file_name == "DLTINS_20210117.zip"
        assert result[0].id == "FILE001"

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url")
            with pytest.raises(requests.exceptions.HTTPError):
                fetcher.extract_xml_file_metadata()

    def test_raises_on_network_error(self):
        with patch(
            "app.xml_fetcher.requests.get",
            side_effect=requests.exceptions.ConnectionError("unreachable"),
        ):
            fetcher = XMLFetcher("http://test.url")
            with pytest.raises(requests.exceptions.ConnectionError):
                fetcher.extract_xml_file_metadata()

    def test_raises_on_invalid_xml_response(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"not valid xml <<<"

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url")
            with pytest.raises(Exception):
                fetcher.extract_xml_file_metadata()

    def test_returns_empty_list_when_no_docs(self):
        empty_xml = b"""<?xml version="1.0"?><response><result></result></response>"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = empty_xml

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url")
            result = fetcher.extract_xml_file_metadata()

        assert result == []


class TestDownloadXmlFile:
    def test_extracts_zip_contents_to_folder(self, tmp_path):
        zip_bytes = make_zip_bytes("data.xml", b"<root/>")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
            fetcher.download_xml_file("http://example.com/file.zip")

        assert (tmp_path / "data.xml").exists()

    def test_raises_on_http_error(self, tmp_path):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500")

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
            with pytest.raises(requests.exceptions.HTTPError):
                fetcher.download_xml_file("http://example.com/file.zip")

    def test_raises_on_network_error(self, tmp_path):
        with patch(
            "app.xml_fetcher.requests.get",
            side_effect=requests.exceptions.ConnectionError("unreachable"),
        ):
            fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
            with pytest.raises(requests.exceptions.ConnectionError):
                fetcher.download_xml_file("http://example.com/file.zip")

    def test_raises_on_bad_zip(self, tmp_path):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"this is not a zip file"

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
            with pytest.raises(zipfile.BadZipFile):
                fetcher.download_xml_file("http://example.com/file.zip")

    def test_raises_on_unexpected_request_error(self, tmp_path):
        with patch(
            "app.xml_fetcher.requests.get",
            side_effect=RuntimeError("unexpected"),
        ):
            fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
            with pytest.raises(RuntimeError):
                fetcher.download_xml_file("http://example.com/file.zip")

    def test_raises_on_unexpected_extraction_error(self, tmp_path):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = make_zip_bytes("data.xml", b"<root/>")

        with patch("app.xml_fetcher.requests.get", return_value=mock_response):
            with patch("app.xml_fetcher.zipfile.ZipFile", side_effect=RuntimeError("extraction failed")):
                fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
                with pytest.raises(RuntimeError, match="extraction failed"):
                    fetcher.download_xml_file("http://example.com/file.zip")


class TestListDownloadedXmlFiles:
    def test_returns_only_xml_files(self, tmp_path):
        (tmp_path / "file1.xml").write_text("<xml/>")
        (tmp_path / "file2.xml").write_text("<xml/>")
        (tmp_path / "readme.txt").write_text("ignore me")

        fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
        result = fetcher.list_downloaded_xml_files()

        assert sorted(result) == ["file1.xml", "file2.xml"]

    def test_returns_empty_list_when_no_xml_files(self, tmp_path):
        (tmp_path / "readme.txt").write_text("ignore me")

        fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
        result = fetcher.list_downloaded_xml_files()

        assert result == []

    def test_returns_empty_list_for_empty_folder(self, tmp_path):
        fetcher = XMLFetcher("http://test.url", folder_name=str(tmp_path))
        result = fetcher.list_downloaded_xml_files()

        assert result == []
