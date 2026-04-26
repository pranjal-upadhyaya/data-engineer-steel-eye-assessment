import io
import os
import zipfile
from typing import List

import requests
from lxml import etree

from app.config import app_config
from app.model import ESMARegistersFileModel
from app.utils.logging import logger


class XMLFetcher:
    """Handles fetching file metadata from the ESMA FIRDS API and downloading XML zip files."""

    def __init__(self, url: str, folder_name: str = app_config.xml_folder) -> None:
        """Initialise the fetcher with the API URL and the target download folder.

        Args:
            url: The ESMA FIRDS Solr API URL to fetch file metadata from.
            folder_name: Relative path to the folder where XML files will be extracted.
        """
        self.url = url
        self.folder_name = folder_name
        self.download_path = os.path.join(os.getcwd(), self.folder_name)

    def extract_xml_file_metadata(self) -> List[ESMARegistersFileModel]:
        """Fetch the ESMA FIRDS API and parse file metadata from the XML response.

        Returns:
            A list of ESMARegistersFileModel instances, one per file record in the response.
        """
        output = []
        response = requests.get(url=self.url)
        logger.info(f"Fetched metadata — HTTP {response.status_code}")
        root = etree.fromstring(response.content)
        docs = root.xpath("/response/result/doc")
        logger.info(f"Found {len(docs)} file records")
        for doc in docs:
            file_metadata = {child.get("name"): child.text for child in doc}
            file_metadata = ESMARegistersFileModel.model_validate(file_metadata)
            output.append(file_metadata)
        return output

    def download_xml_file(self, url: str) -> None:
        """Download a zip file from the given URL and extract its contents to the download folder.

        Args:
            url: Direct download URL of the zip file.
        """
        response = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(self.download_path)
        logger.info(f"Extracted zip to {self.download_path}")

    def download_xml_files(
        self, xml_file_metadata_list: List[ESMARegistersFileModel]
    ) -> None:
        """Download zip files for all file records in the provided metadata list.

        Args:
            xml_file_metadata_list: List of ESMARegistersFileModel instances to download.
        """
        for metadata in xml_file_metadata_list:
            download_link = metadata.download_link
            self.download_xml_file(url=download_link)
            logger.info(f"Successfully downloaded XML file from: {download_link}")

    def extract_and_download_xml_files(self) -> None:
        """Fetch file metadata from the API and download all listed zip files."""
        xml_file_metadata_list = self.extract_xml_file_metadata()
        self.download_xml_files(xml_file_metadata_list)

    def list_downloaded_xml_files(self) -> List[str]:
        """List all XML files present in the download folder.

        Returns:
            A list of XML filenames found in the download folder.
        """
        files = os.listdir(self.download_path)
        xml_files = [file for file in files if file.endswith(".xml")]
        return xml_files
