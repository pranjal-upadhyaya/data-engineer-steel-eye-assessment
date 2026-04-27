from app.pipeline import XMLExtractorAndParser

if __name__ == "__main__":
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
    xml_fetcher_and_parser = XMLExtractorAndParser(url, file_index=4)
    xml_fetcher_and_parser.run()
