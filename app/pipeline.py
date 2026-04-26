from typing import List
from app.xml_fetcher import XMLFetcher
from app.xml_parser import XMLParser
from app.model import ESMARegistersFileModel
from app.config import app_config
import os


class XMLExtractorAndParser:
    """Orchestrates the full pipeline: fetching metadata, downloading XML files, and parsing them to CSV."""

    def __init__(self, url: str, xml_folder_name: str = app_config.xml_folder) -> None:
        """Initialise the pipeline with the ESMA API URL and the XML download folder.

        Args:
            url: The ESMA FIRDS Solr API URL to fetch file metadata from.
            xml_folder_name: Relative path to the folder where XML files are stored after download.
        """
        self.url = url
        self.xml_folder_name = xml_folder_name
        self.xml_folder_path = os.path.join(os.getcwd(), self.xml_folder_name)
        self.file_index = 1

    def get_dltins_file_by_index(self, metadata_list: List[ESMARegistersFileModel]) -> ESMARegistersFileModel:
        """Filter the metadata list for DLTINS file types and return the entry at the configured index.

        Args:
            metadata_list: Full list of file metadata records returned by the API.

        Returns:
            The ESMARegistersFileModel at position ``file_index`` among DLTINS entries.
        """
        dltins_files = [m for m in metadata_list if m.file_type == "DLTINS"]
        return dltins_files[self.file_index]

    def run(self) -> None:
        """Execute the full pipeline: fetch metadata, download the target DLTINS file, and parse it to CSV."""
        fetcher = XMLFetcher(self.url)

        metadata_list = fetcher.extract_xml_file_metadata()
        target = self.get_dltins_file_by_index(metadata_list)

        fetcher.download_xml_file(target.download_link)
        downloaded_xml_files = fetcher.list_downloaded_xml_files()
        print(downloaded_xml_files)

        for xml_file in downloaded_xml_files:
            xml_file_path = os.path.join(self.xml_folder_path, xml_file)
            print(xml_file_path)
            parser = XMLParser(xml_file_path)
            parser.extract_xml_from_file()
