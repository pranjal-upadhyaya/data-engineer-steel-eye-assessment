import os
from typing import Any, List

import pandas as pd
from lxml import etree

from app.config import app_config
from app.utils.logging import logger


class XMLParser:
    """Parses a large DLTINS XML file in batches and writes the extracted data to CSV."""

    def __init__(
        self, file_path: str, csv_folder_name: str = app_config.csv_folder
    ) -> None:
        """Initialise the parser with the XML file path and the target CSV output folder.

        Args:
            file_path: Absolute path to the XML file to be parsed.
            csv_folder_name: Relative path to the folder where CSV output files will be written.
        """
        self.xml_file_path = file_path
        self.csv_folder_name = csv_folder_name
        self.csv_folder_path = os.path.join(os.getcwd(), self.csv_folder_name)
        os.makedirs(self.csv_folder_path, exist_ok=True)
        self.first_write = True
        self.batch_size = 1000

    def extract_xml_from_file(self) -> None:
        """Stream-parse the XML file using iterparse and trigger data extraction."""
        try:
            context = etree.iterparse(
                self.xml_file_path,
                events=("start", "end"),
                tag="{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd",
            )
            self.extract_xml_data(context)
            del context
        except FileNotFoundError as e:
            logger.error(f"XML file not found: {self.xml_file_path}: {e}")
            raise
        except etree.XMLSyntaxError as e:
            logger.error(f"Failed to parse XML file {self.xml_file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error parsing {self.xml_file_path}: {e}")
            raise

    def extract_xml_data(self, context: Any) -> List[dict]:
        """Iterate over parsed XML elements, extract fields, and write in batches to CSV.

        Args:
            context: An lxml iterparse context yielding (event, element) tuples.

        Returns:
            An empty list after all data has been flushed to CSV.
        """
        data = []
        batch_number = 1
        element_count = 0
        for event, elem in context:
            if event == "end":
                element_count += 1
                data_set = {}
                namespace = elem.tag.split("}")[0] + "}"

                fin = elem.find(f"{namespace}FinInstrmGnlAttrbts")
                tag_list = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]
                for tag in tag_list:
                    val = self.extract_data_from_xml_ele(fin, namespace, tag)
                    data_set[f"FinInstrmGnlAttrbts.{tag}"] = val

                val = self.extract_data_from_xml_ele(elem, namespace, "Issr")
                data_set["Issr"] = val

                data.append(data_set)
                if len(data) >= self.batch_size:
                    self.dump_xml_data_to_csv(data)
                    data = []
                    logger.info(f"Processed batch {batch_number} with {element_count} elements")
                    batch_number += 1
                    element_count = 0

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        if len(data) > 0:
            self.dump_xml_data_to_csv(data)
            data = []
            logger.info(f"Processed batch {batch_number} with {element_count} elements")
        return data

    def process_xml_data(self, data: List[dict]) -> pd.DataFrame:
        """Convert a list of record dicts to a DataFrame and add derived columns.

        Adds:
            - ``a_count``: number of lowercase 'a' characters in ``FinInstrmGnlAttrbts.FullNm``.
            - ``contains_a``: 'YES' if ``a_count`` > 0, 'NO' otherwise.

        Args:
            data: List of dicts, each representing one parsed XML record.

        Returns:
            A pandas DataFrame with the original fields and the two derived columns.
        """
        df = pd.DataFrame(data)
        df["a_count"] = df["FinInstrmGnlAttrbts.FullNm"].fillna("").str.count("a")
        df["contains_a"] = df["a_count"].apply(lambda x: "YES" if x > 0 else "NO")
        return df

    def dump_xml_data_to_csv(self, data: List[dict]) -> None:
        """Process a batch of records and append them to the output CSV file.

        Writes the header on the first call and appends without header on subsequent calls.

        Args:
            data: List of dicts representing one batch of parsed XML records.
        """
        try:
            df = self.process_xml_data(data)
            write_mode = "w" if self.first_write else "a"
            file_name = os.path.basename(self.xml_file_path)
            file_path = os.path.join(self.csv_folder_path, f"{file_name}.csv")
            df.to_csv(file_path, mode=write_mode, header=self.first_write, index=False)
            self.first_write = False
        except OSError as e:
            logger.error(f"Failed to write CSV to {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing CSV: {e}")
            raise

    def extract_data_from_xml_ele(
        self, parent_element: Any, namespace: str, tag: str
    ) -> str | None:
        """Extract the text value of a child element within a given parent element.

        Args:
            parent_element: The lxml element to search within.
            namespace: The XML namespace string including braces, e.g. '{urn:...}'.
            tag: The local tag name of the child element to find.

        Returns:
            The text content of the child element, or None if the element is not found.
        """
        element = parent_element.find(f"{namespace}{tag}")
        value = element.text if element is not None else None
        return value
