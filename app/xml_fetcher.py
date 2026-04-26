from typing import List
import requests, zipfile, io, os
from lxml import etree
from app.model import ESMARegistersFileModel

class XMLFetcher:

    def __init__(self, url, folder_name) -> None:
        self.url = url
        self.folder_name = folder_name

    def extract_xml_file_metadata(self):

        output = []
        response = requests.get(
            url=self.url
        )
        print(response.status_code)
        content = response.text
        root = etree.fromstring(response.content)
        print(content)
        print("=====================")
        docs = root.xpath("/response/result/doc") 
        print(docs)
        for doc in docs:
            file_metadata = {child.get("name"): child.text for child in doc}
            file_metadata = ESMARegistersFileModel.model_validate(file_metadata)
            print(file_metadata)
            output.append(file_metadata)

        return output

    def download_xml_file(self, url: str):
        response = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        cwd = os.getcwd()
        download_path = os.path.join(cwd, self.folder_name)
        z.extractall(download_path)
        print("=====================")
        return

    def download_xml_files(self, xml_file_metadata_list: List[ESMARegistersFileModel]):
        for metadata in xml_file_metadata_list:
            download_link = metadata.download_link
            self.download_xml_file(url = download_link)
            print(f"Successfully downloaded xml file from link: {download_link}")

        return

    def extract_and_download_xml_files(self):
        xml_file_metadata_list = self.extract_xml_file_metadata()
        self.download_xml_files(xml_file_metadata_list)
        return

    def list_downloaded_xml_files(self):
        cwd = os.getcwd()
        download_path = os.path.join(cwd, self.folder_name)
        files = os.listdir(download_path)
        xml_files = [file for file in files if file.endswith(".xml")]
        return xml_files