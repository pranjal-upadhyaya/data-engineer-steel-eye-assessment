from typing import Any, List
from lxml import etree
import pandas as pd
import csv


class XMLParser:

    def __init__(self, file_path) -> None:
        self.file_path = file_path
        self.first_write = True
        self.batch_size = 1000


    def extract_xml_from_file(self):
        context = etree.iterparse(self.file_path, events=("start", "end"), tag="{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd")

        data = self.extract_xml_data(context)

        del context
    

    def extract_xml_data(self, context: Any):
        data = []
        for event, elem in context:
            if event == "start":
                print("========================")
            print(event, elem.tag)
            if event == "end":
                data_set = {}
                namespace = elem.tag.split("}")[0]+"}"
                print(namespace)
                fin = elem.find(f"{namespace}FinInstrmGnlAttrbts")
                tag_list = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]
                for tag in tag_list:
                    val = self.extract_data_from_xml_ele(fin, namespace, tag)
                    data_set[f"FinInstrmGnlAttrbts.{tag}"] = val
                val = self.extract_data_from_xml_ele(elem, namespace, "Issr")
                data_set[f"Issr"] = val
                print(data_set)
                data.append(data_set)
                if len(data) >= self.batch_size:
                    self.dump_xml_data_to_csv(data)
                    data = []
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                print("========================")

        if len(data) > 0:
            self.dump_xml_data_to_csv(data)
            data = []
        return data

    def dump_xml_data_to_csv(self, data: List[dict]):
        df = pd.DataFrame(data)

        if self.first_write:
            with open("xml_data_dump.csv", "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])
                writer.writeheader()
                writer.writerows(data)
            self.first_write = False
        else:
            with open("xml_data_dump.csv", "a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])
                writer.writerows(data)

        return None

    def extract_data_from_xml_ele(self, parent_element: Any, namespace: str, tag: str):
        element = parent_element.find(f"{namespace}{tag}")
        value = element.text if element is not None else None
        return value
