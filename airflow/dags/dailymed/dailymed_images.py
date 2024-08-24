import os
import pandas as pd
from dailymed.dailymed import DailyMed
from xml_functions import parse_dm_xml_to_dict
from sagerx import load_df_to_pg
import logging


class DailyMedImages(DailyMed):
    def __init__(self, data_folder: os.PathLike) -> None:
        super().__init__(data_folder)

    def get_full_ndc_variants(self, ndcs):
        ndcs_11 = [self.convert_ndc_10_to_11(ndc) for ndc in ndcs]
        ndcs.extend(ndcs_11)
        ndcs_nd = [self.convert_ndc_no_dash(ndc) for ndc in ndcs]
        ndcs.extend(ndcs_nd)
        ndcs.sort(key=lambda s: len(s), reverse=True)
        return ndcs
    
    def get_ndc_from_image_filename(self, ndc_variants, image_id):
        # attempt to regex an NDC from image file name
        image_ndc = self.ndc_format(image_id)

        # if NDC match found
        if image_ndc:
            # compare it against all valid NDC variants in SPL
            # TODO: convert ndc_variants to a dict and iterate
            # through items so that it compares to the list of variants,
            # but returns the original NDC that is represnted by those variants
            for ndc in ndc_variants:
                if ndc == image_ndc:
                    return ndc
            # if no valid NDC variant match, assume it is a
            # random NDC-length number and disregard match
            return None
        # if no NDC match found in image file name, return None
        else:
            return None
        
    def find_image_components(self,xml_doc):
        components = []
        for component in xml_doc['document']['component']['structuredBody']['component']:
            if component['section']['code']['@code'] == '51945-4':
                components.append(component)
        return components

    def find_ndcs_in_component(self, component):
        ndcs = self.ndc_format(component['Text'][0])
        # NOTE: may need to modify ndc_format function to return multiple matches
        # for now, just returning a list containing the single response
        return [ndcs]
    
    def get_ndcs_from_image_components(self, xml_doc, ndc_ids, image_ids):
        mapped_dict = {}

        image_components = [xml_doc] # leaving as a list because we need to modify the XSL to find multiple PRIMARY DISPLAY PANELS

        if image_components == []:
            return None 

        for component in image_components:
            ndcs = self.find_ndcs_in_component(component)
            images = component['Image']
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
            # get all image file names associated with the SPL
            image_files = mapping.get('image_files')
            print(image_files)
            # get all NDCs associated with the SPL
            ndcs = mapping.get('ndcIds')
            print(ndcs)
            # get all variants of each NDC to check against potential
            # different formatting in the image name
            # TODO: reconfigure this as a dict so that the original NDC
            # points to a list of all variations of itself, including itself
            ndc_variants = self.get_full_ndc_variants(ndcs)            

            for image_file in image_files:
                # attempt to regex an NDC out of each image
                # and also ensure that the NDC matches an NDC
                # from the SPL - not a random NDC-length number
                matched_ndc = self.get_ndc_from_image_filename(ndc_variants, image_file)
                
                # if a match is found, add it to a mapping dict
                if matched_ndc:
                    image_ndc_mapping[matched_ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'methodology':'image_filename',
                        'confidence_level':1,
                        'matched':1
                    }
            
            # add un-matched NDCs to the list
            # NOTE: maybe instead, we add un-matched images to the list?
            for ndc in ndcs:
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'methodology':'image_filename',
                        'confidence_level':1,
                        'matched':0
                    }
            
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
            xml_doc = self.find_xml_package_data(xml_file_path)
            #xml_doc = parse_dm_xml_to_dict(xml_file_path)
            #print(xml_doc)
            logging.debug(xml_file_path)
            
            if ('Image' in xml_doc):
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
