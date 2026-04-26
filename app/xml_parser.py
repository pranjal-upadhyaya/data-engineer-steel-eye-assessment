from typing import Any, List
from lxml import etree
import pandas as pd
import os
from app.config import app_config


class XMLParser:

    def __init__(self, file_path: str, csv_folder_name: str = app_config.csv_folder) -> None:
        self.xml_file_path = file_path
        self.csv_folder_name = csv_folder_name
        self.csv_folder_path = os.path.join(os.getcwd(), self.csv_folder_name)
        os.makedirs(self.csv_folder_path, exist_ok=True)
        self.first_write = True
        self.batch_size = 1000


    def extract_xml_from_file(self):
        context = etree.iterparse(self.xml_file_path, events=("start", "end"), tag="{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd")

        self.extract_xml_data(context)

        del context

        return None


    def extract_xml_data(self, context: Any):
        data = []
        batch_number = 1
        element_count = 0
        for event, elem in context:
            
            if event == "end":
                element_count += 1
                data_set = {}
                namespace = elem.tag.split("}")[0]+"}"
                
                fin = elem.find(f"{namespace}FinInstrmGnlAttrbts")
                tag_list = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]
                for tag in tag_list:
                    val = self.extract_data_from_xml_ele(fin, namespace, tag)
                    data_set[f"FinInstrmGnlAttrbts.{tag}"] = val
                
                val = self.extract_data_from_xml_ele(elem, namespace, "Issr")
                data_set[f"Issr"] = val
                
                data.append(data_set)
                if len(data) >= self.batch_size:
                    self.dump_xml_data_to_csv(data)
                    data = []
                    print(f"Processed batch {batch_number} with {element_count} elements")
                    batch_number += 1
                    element_count = 0
                
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        if len(data) > 0:
            self.dump_xml_data_to_csv(data)
            data = []
            print(f"Processed batch: {batch_number} with {element_count} elements")
        return data

    def process_xml_data(self, data: List[dict]):
        df = pd.DataFrame(data)
        df["a_count"] = df["FinInstrmGnlAttrbts.FullNm"].fillna("").str.count("a")
        df["contains_a"] = df["a_count"].apply(lambda x: "YES" if x > 0 else "NO")
        return df

    def dump_xml_data_to_csv(self, data: List[dict]):

        df = self.process_xml_data(data)

        write_mode = "w" if self.first_write else "a"

        file_name = os.path.basename(self.xml_file_path)

        file_path = os.path.join(self.csv_folder_path, f"{file_name}.csv")

        df.to_csv(file_path, mode = write_mode)

        self.first_write = False

        return None

    def extract_data_from_xml_ele(self, parent_element: Any, namespace: str, tag: str):
        element = parent_element.find(f"{namespace}{tag}")
        value = element.text if element is not None else None
        return value
