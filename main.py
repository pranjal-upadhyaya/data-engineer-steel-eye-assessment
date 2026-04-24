from lxml import etree

import requests


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

if __name__ == "__main__":
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    extract_xml(url)