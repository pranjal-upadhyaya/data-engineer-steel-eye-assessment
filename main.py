from typing import Any, List
from lxml import etree
import requests, zipfile, io, os
import pandas as pd
from app import xml_parser
from app.xml_parser import XMLParser
from app.xml_fetcher import XMLFetcher
from app.model import ESMARegistersFileModel


def extract_xml(url: str):

    response = requests.get(
        url=url
    )
    print(response.status_code)
    content = response.text
    root = etree.fromstring(response.content)
    print(content)
    print("=====================")
    docs = root.xpath("/response/result/doc") 
    print(docs)
    for doc in docs:
        data = {child.get("name"): child.text for child in doc}
        print(data)

def extract_xml_1(url: str):
    response = requests.get(
        url=url
    )
    print(response.status_code)
    content = response.text
    # root = etree.fromstring(response.content)
    print(content)
    print("=====================")
    # docs = root.xpath("/response/result/doc") 
    # print(docs)
    # for doc in docs:
    #     data = {child.get("name"): child.text for child in doc}
    #     print(data)

def extract_xml_struct_from_file(file_path: str):
    context = etree.iterparse(file_path, events=("start", "end"))

    loop = 100
    for event, elem in context:
        print(event, elem.tag)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        print("========================")
        
        loop -= 1
        if loop < 0:
            break
    del context

def download_file(url: str):
    response = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    cwd = os.getcwd()
    download_path = os.path.join(cwd, "temp")
    z.extractall(download_path)
    print("=====================")
    return download_path



if __name__ == "__main__":
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    xml_fetcher = XMLFetcher(url)
    xml_fetcher.extract_and_download_xml_files()
    # url_1 = "https://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip"
    # download_path = download_file(url_1)
    download_path = "/Users/pranjal/code/data-engineer-steel-eye-assessment/temp"
    print(download_path)
    # file_name = os.path.join(download_path, "DLTINS_20210117_01of01.xml")
    # xml_parser = XMLParser(file_name)
    # xml_parser.extract_xml_from_file()