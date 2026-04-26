from typing import List
from app.xml_fetcher import XMLFetcher
from app.xml_parser import XMLParser
from app.model import ESMARegistersFileModel
import os


class XMLExtractorAndParser:

    def __init__(self, url: str) -> None:
        self.url = url
        self.folder_name = "temp"
        self.file_index = 1

    def get_dltins_file_by_index(self, metadata_list: List[ESMARegistersFileModel]) -> ESMARegistersFileModel:
        dltins_files = [m for m in metadata_list if m.file_type == "DLTINS"]
        return dltins_files[self.file_index]

    def run(self) -> None:
        fetcher = XMLFetcher(self.url, self.folder_name)

        metadata_list = fetcher.extract_xml_file_metadata()
        target = self.get_dltins_file_by_index(metadata_list)

        fetcher.download_xml_file(target.download_link)
        downloaded_xml_files = fetcher.list_downloaded_xml_files()

        for xml_file in downloaded_xml_files:
            xml_file_path = os.path.join(os.getcwd(), self.folder_name, xml_file)
            parser = XMLParser(xml_file_path)
            parser.extract_xml_from_file()
