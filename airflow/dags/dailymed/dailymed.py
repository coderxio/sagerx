import os
from pathlib import Path
from xml_functions import transform_xml_to_dict, get_xsl_template_path, transform_xml
import pandas as pd
from sagerx import load_df_to_pg
import logging
from airflow import configuration as conf

class DailyMed():
    def __init__(self, data_folder: os.PathLike) -> None:
        self.data_folder = data_folder
        self.rx_folder = Path(data_folder) / "prescription"

        airflow_logging_level = conf.get('logging', 'logging_level')

        if airflow_logging_level == 'DEBUG':
            logging.debug("This is a debug message that will only be logging.debuged when logging_level is set to DEBUG.")
        else:
            logging.info("This is an info message, but DEBUG level messages will not be logging.debuged.")


    ### 
    # Supplementary Functions
    ### 

    def ndc_format(self,text_data):
        import re

        # order of patterns is important
        # largest to smallest
        patterns = [
        (r'\d{11}', '11 Digit'),
        (r'\d{10}', '10 Digit'),
        (r'\d{5}-\d{5}', '5-5'),
        (r'\d{5}-\d{4}-\d{2}', '5-4-2'),
        (r'\d{5}-\d{4}-\d{1}', '5-4-1'),
        (r'\d{5}-\d{3}-\d{2}', '5-3-2'),
        (r'\d{4}-\d{6}', '4-6'),
        (r'\d{4}-\d{4}-\d{2}', '4-4-2')
        ]

        for pattern, _ in patterns:
            match = re.search(pattern, text_data)
            if match:
                return match.group(0)
        
        return None
    
    def convert_ndc_10_to_11(self,ndc):
        parts = ndc.split('-')
        if len(parts[-1]) == 1:
            parts[-1] = '0' + parts[-1]
        return '-'.join(parts)
    
    def convert_ndc_no_dash(self,ndc):
        return ndc.replace("-","")

    ###
    # XML Processing 
    ###

    def find_xml_image_ids(self, xml_doc) -> list:
        xslt = get_xsl_template_path("package_data.xsl")
        results = transform_xml_to_dict(xml_doc,xslt)
        return list(set(results.get('Image',[])))

    def find_xml_ndc_numbers(self, xml_doc) -> list:
        xslt = get_xsl_template_path("ndcs.xsl")
        results = transform_xml_to_dict(xml_doc,xslt)
        #print(results)
        return list(set(results.get('NDCs', {}).get('NDC', [])))
    
    def find_xml_metadata(self, xml_doc) -> dict:
        xslt = get_xsl_template_path("doc_metadata.xsl")
        results = transform_xml_to_dict(xml_doc,xslt)
        return results
    
    def find_xml_package_data(self, xml_doc) -> dict:
        xslt = get_xsl_template_path("package_data.xsl")
        results = transform_xml_to_dict(xml_doc,xslt)
        return results

    def metadata_dict_cleanup(self, metadata):
        new_dict = {}
        for key, value in metadata.items():
            if isinstance(value, list) and len(value) == 1:
                new_dict[key] = str(value[0])
            elif isinstance(value, list) and len(value) > 1:
                new_dict[key] = value
        return new_dict


    def process_xml_doc(self, xml_doc):
        #print('process_xml_doc')
        image_ids = self.find_xml_image_ids(xml_doc)
        #print('found_xml_image_ids')
        ndc_ids = self.find_xml_ndc_numbers(xml_doc)
        #print('found_ndc_ids')

        metadata = self.find_xml_metadata(xml_doc)
        #print('found_xml_metadata')

        metadata['imageIds'] = image_ids
        metadata['ndcIds'] = ndc_ids
        return metadata

    ### 
    # File Processing
    ###
    
    def unzip_data(self) ->  None:
        import zipfile
        for zip_folder in self.rx_folder.iterdir():
            if zip_folder.is_file() and zip_folder.suffix == '.zip':
                logging.debug(zip_folder)
                with zipfile.ZipFile(zip_folder) as unzipped_folder:
                    folder_name = zip_folder.stem
                    extracted_folder_path = self.rx_folder / folder_name
                    extracted_folder_path.mkdir(exist_ok=True)

                    for subfile in unzipped_folder.infolist():
                        unzipped_folder.extract(subfile, extracted_folder_path)

                os.remove(zip_folder)

    def map_files(self):
        file_mapping ={}
        for spl_folder in self.rx_folder.iterdir():
            if spl_folder.name == '.DS_Store':
                continue

            image_files = []
            xml_file_name = ""

            for subfile in spl_folder.iterdir():
                if subfile.suffix == '.xml':
                    xml_file_name = subfile.name
                elif subfile.suffix == '.jpg':
                    image_files.append(subfile.name)

            spl = spl_folder.name.split("_")[1]
            #print(spl)

            xml_path = self.get_file_path(spl_folder, xml_file_name)
            metadata = self.process_xml_doc(xml_path)

            file_dict = {
                "xml_file":xml_file_name,
                "image_files": image_files,
                "spl_folder_name": spl_folder.name
            }
            file_mapping[spl] = dict(file_dict, **metadata)
        self.file_mapping = file_mapping
        logging.debug(file_mapping)

    
    def get_file_path(self, spl_folder_name, file_name):
        return os.path.join(self.rx_folder,spl_folder_name,file_name)

    ###
    # Data Extraction for DailyMed Daily
    ###

    def extract_and_upload_dmd_base_data(self):
        xslt = get_xsl_template_path("dailymed_prescription.xsl")

            
        for spl, mapping in self.file_mapping.items():
            spl_folder_name = mapping.get("spl_folder_name")
            xml_file = self.get_file_path(spl_folder_name, mapping.get("xml_file"))
            xml_content = transform_xml(xml_file, xslt)

            df = pd.DataFrame(
                columns=["spl","spl_folder_name", "xml_file_name", "xml_content","image_files"],
                data=[[spl, spl_folder_name, mapping.get("xml_file"), xml_content, mapping.get("image_files")]],
            )

            load_df_to_pg(df,"sagerx_lake","dailymed_daily","append")