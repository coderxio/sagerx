import os
import pandas as pd
from dailymed.dailymed import DailyMed
from xml_functions import parse_dm_xml_to_dict
from sagerx import load_df_to_pg
import logging


class DailyMedImages(DailyMed):
    def __init__(self, data_folder: os.PathLike) -> None:
        super().__init__(data_folder)

    def create_dailymed_image_url(self, image_id, spl):
        return f"https://dailymed.nlm.nih.gov/dailymed/image.cfm?name={image_id}&setid={spl}&type=img"


    def has_none_values(self,d):
        for value in d.values():
            if value is {}:
                return True
        return False
    
    def validate_ndc(self,ndc):
        """"
        Validates that the NDC is greater than 9 characters
        """
        raw_ndc = ndc.replace("-","")
        if len(raw_ndc) > 9:
            return True
        return False

    """
    {'1d9f4044-a333-ecd3-e063-6294a90ab1fe': 
    {'xml_file': '1e004cc6-580a-1e62-e063-6294a90aa220.xml',
    'image_files': ['Xiclofen Box.jpg', 'Xiclofen Tube.jpg'], 
    'spl_folder_name': '20240725_1d9f4044-a333-ecd3-e063-6294a90ab1fe',
    'documentId': '1e004cc6-580a-1e62-e063-6294a90aa220',
    'SetId': '1d9f4044-a333-ecd3-e063-6294a90ab1fe', 
    'VersionNumber': '3', 
    'EffectiveDate': '20240724', 
    'MarketStatus': 'unapproved drug other', 
    'imageIds': ['Xiclofen Box.jpg', 'Xiclofen Tube.jpg'], 
    'ndcIds': '83295-5000-1'}}
    """

    def get_full_ndc_variants(self, ndcs):
        ndcs_11 = [self.convert_ndc_10_to_11(ndc) for ndc in ndcs]
        ndcs.extend(ndcs_11)
        ndcs_nd = [self.convert_ndc_no_dash(ndc) for ndc in ndcs]
        ndcs.extend(ndcs_nd)
        ndcs.sort(key=lambda s: len(s), reverse=True)
        return ndcs
    
    def get_ndc_from_image_filename(self, ndcs, image_id):
        image_ndc = self.ndc_format(image_id)

        logging.debug(f"Got NDC varients, total {len(ndcs)}")
        logging.debug(f"image ndc: {image_ndc} from {image_id}")

        if image_ndc: 
            for ndc in ndcs:
                if ndc == image_ndc:
                    return ndc
            return None

        else:
            #logging.debug(f"NDC {image_ndc} from {id} not found in NDC list of: {ndcs}")
            return None
        
    def find_image_components(self,xml_doc):
        components = []
        for component in xml_doc['document']['component']['structuredBody']['component']:
            if component['section']['code']['@code'] == '51945-4':
                components.append(component)
        return components

    def find_ndcs_in_component(self, component, results=None):
        if results is None:
            results = []

        if isinstance(component, str) and self.ndc_format(component):
            results.append(self.ndc_format(component))
        elif isinstance(component, dict):
            if '#text' in component and self.ndc_format(component['#text']):
                results.append(self.ndc_format(component['#text']))
            for _, value in component.items():
                    if isinstance(value, (dict, list)):
                        self.find_ndcs_in_component(value, results)
        elif isinstance(component, list):
            for item in component:
                self.find_ndcs_in_component(item,results)
        return list(set(results))
    
    def find_images_in_component(self, xml_doc, results=None):
        if results is None:
            results = []

        if isinstance(xml_doc, dict):
            if '@mediaType' in xml_doc and xml_doc['@mediaType'] == 'image/jpeg':
                results.append(xml_doc.get('reference', {}).get('@value'))
            for _, value in xml_doc.items():
                if isinstance(value, (dict, list)):
                    self.find_images_in_component(value, results)
        elif isinstance(xml_doc, list):
            for item in xml_doc:
                self.find_images_in_component(item, results)
        return list(set(results))

    def get_ndcs_from_image_components(self,xml_doc, ndc_ids, image_ids):
        mapped_dict = {}
        image_components = self.find_image_components(xml_doc)

        if image_components == []:
            return None 

        for component in image_components:
            ndcs = self.find_ndcs_in_component(component)
            images = self.find_images_in_component(component)

            if not ndcs or not images:
                continue

            elif len(ndcs) == 1 and len(images) == 1:
                ndc = ndcs[0]
                image = images[0]

                if ndc in ndc_ids and image in image_ids:
                    mapped_dict[ndc] = image
                else:
                    logging.debug(f"Found unknown ndc or image")
                    logging.debug(f"NDC {ndc}, vs expected {ndc_ids}")
                    logging.debug(f"Image {image}, vs expected {image_ids}")
        return mapped_dict


    def extract_and_upload_mapped_ndcs_from_image_files(self):
        mapping_dict = self.file_mapping
        image_ndc_mapping = {}

        for spl, mapping in mapping_dict.items():
            
            ndcs = mapping.get('ndcIds')
            ndc_variants = self.get_full_ndc_variants(ndcs)
            image_files = mapping.get('image_files')

            logging.debug(f"image file check for {spl}")
            logging.debug(f"NDCs found from mapping: {len(ndcs)}")
            logging.debug(f"NDC varients: {len(ndc_variants)}")

            # Get NDCs when found in the image filenames 
            logging.debug(f"Found {len(image_files)} image files")

            for image_file in image_files:
                matched_ndc = self.get_ndc_from_image_filename(ndc_variants, image_file)

                logging.debug(f"Mapping dict length of: {len(image_ndc_mapping)}")

                if matched_ndc:
                    image_ndc_mapping[matched_ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'image_url':self.create_dailymed_image_url(image_file, spl),
                        'methodology':'image_filename',
                        'confidence_level':1,
                        'matched':1} 
            
            logging.debug(f"NDCs found from mapping post: {len(mapping.get('ndcIds'))}")

            for ndc in mapping.get('ndcIds'):
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'image_url':'',
                        'methodology':'image_filename',
                        'confidence_level':1,
                        'matched':0} 
                    
            logging.debug(f"Mapping keys: {image_ndc_mapping.keys()}")

            
        df = pd.DataFrame.from_dict(image_ndc_mapping, orient='index')
        df = df.reset_index().rename(columns={'index':'ndc'})
        load_df_to_pg(df,"sagerx_lake","dailymed_image_ndc","append")


    def extract_and_upload_mapped_ndcs_from_image_components(self):
        mapping_dict = self.file_mapping
        image_ndc_mapping = {}

        for spl, mapping in mapping_dict.items():
            logging.debug(f"image component check for {spl}")

            ndcs = mapping.get('ndcIds')
            image_files = mapping.get('image_files')

            # Get NDCs from XML components
            spl_folder_name = mapping.get("spl_folder_name")
            xml_file_path = self.get_file_path(spl_folder_name, mapping.get("xml_file"))
            xml_doc = parse_dm_xml_to_dict(xml_file_path)
            logging.debug(xml_file_path)
            
            matched_components = self.get_ndcs_from_image_components(xml_doc, ndcs, image_files)

            for ndc,image_file in matched_components.items():
                image_ndc_mapping[ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'image_url':self.create_dailymed_image_url(image_file, spl),
                        'methodology':'image_component',
                        'confidence_level':0.75,
                        'matched':1} 
            
            for ndc in ndcs:
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'image_url':'',
                        'methodology':'image_filename',
                        'confidence_level':1,
                        'matched':0} 
                
        df = pd.DataFrame.from_dict(image_ndc_mapping, orient='index')
        df = df.reset_index().rename(columns={'index':'ndc'})
        load_df_to_pg(df,"sagerx_lake","dailymed_image_ndc","append")


    def barcode_to_ndc(self,data):
        if len(data) > 11:
            data = data[:-1]
            data = data[2:]

        if len(data) == 10:
            data = data[:-1] + '0' + data[-1]
        
        return data

    def extract_and_upload_mapped_ndcs_from_image_barcode(self):
        from PIL import Image, ImageOps
        from pyzbar.pyzbar import decode

        mapping_dict = self.file_mapping
        image_ndc_mapping = {}

        for spl, mapping in mapping_dict.items():
            logging.debug(f"image barcode check for {spl}")

            ndcs = mapping.get('ndcIds')
            ndcs = self.get_full_ndc_varients(ndcs)
            image_files = mapping.get('image_files')

            spl_folder_name = mapping.get("spl_folder_name")

            for image_file in image_files:
                image_file_path = self.get_file_path(spl_folder_name, image_file)
                
                img = Image.open(image_file_path)
                img = img.convert('L')  
                img = ImageOps.autocontrast(img)
                barcodes = decode(img)
        
                if not barcodes:
                    logging.debug("No barcode found in the image.")
                    return
                
                for barcode in barcodes:
                    barcode_ndc  = self.barcode_to_ndc(barcode)
                    if barcode_ndc in ndcs:
                        image_ndc_mapping[barcode_ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'image_url':self.create_dailymed_image_url(image_file, spl),
                        'methodology':'image_barcode',
                        'confidence_level':0.5,
                        'matched':1} 

            for ndc in ndcs:
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'image_url':'',
                        'methodology':'image_barcode',
                        'confidence_level':0.5,
                        'matched':0} 
                    
        df = pd.DataFrame.from_dict(image_ndc_mapping, orient='index')
        df = df.reset_index().rename(columns={'index':'ndc'})
        load_df_to_pg(df,"sagerx_lake","dailymed_image_ndc","append")



    def extract_and_upload_mapped_ndcs_from_image_ocr(self):
        import pytesseract
        from PIL import Image

        mapping_dict = self.file_mapping
        image_ndc_mapping = {}

        for spl, mapping in mapping_dict.items():
            logging.debug(f"image OCR check for {spl}")

            ndcs = mapping.get('ndcIds')
            ndcs = self.get_full_ndc_varients(ndcs)
            image_files = mapping.get('image_files')

            spl_folder_name = mapping.get("spl_folder_name")

            for image_file in image_files:
                image_file_path = self.get_file_path(spl_folder_name, image_file)
                
                img = Image.open(image_file_path)
                ocr_text = pytesseract.image_to_string(img)
                lines = ocr_text.splitlines()

                for line in lines:
                    matched_ndc = self.ndc_format(line)

                    if matched_ndc:
                        image_ndc_mapping[matched_ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'image_url':self.create_dailymed_image_url(image_file, spl),
                        'methodology':'image_ocr',
                        'confidence_level':0.25,
                        'matched':1} 
        
        for ndc in ndcs:
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'image_url':'',
                        'methodology':'image_ocr',
                        'confidence_level':0.25,
                        'matched':0} 
                    
        df = pd.DataFrame.from_dict(image_ndc_mapping, orient='index')
        df = df.reset_index().rename(columns={'index':'ndc'})
        load_df_to_pg(df,"sagerx_lake","dailymed_image_ndc","append")
