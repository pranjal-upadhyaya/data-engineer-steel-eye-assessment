from typing import List
import requests, zipfile, io, os
from lxml import etree
from app.model import ESMARegistersFileModel

class XMLFetcher:

    def __init__(self, url) -> None:
        self.url = url

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
        download_path = os.path.join(cwd, "temp")
        z.extractall(download_path)
        print("=====================")
        return download_path

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