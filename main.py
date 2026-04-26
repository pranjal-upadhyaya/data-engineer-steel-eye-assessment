from lxml import etree

from app.pipeline import XMLExtractorAndParser


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


if __name__ == "__main__":
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    xml_fetcher_and_parser = XMLExtractorAndParser(url)
    xml_fetcher_and_parser.run()
