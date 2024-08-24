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
        ndc_matches = self.ndc_format(image_id)

        # if NDC match found
        if ndc_matches:
            image_ndc = ndc_matches[0]
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
    
    def get_ndcs_from_image_components(self, xml_doc, ndc_ids, image_ids):
        mapped_dict = {}
        #print(xml_doc)
        packages = xml_doc.get('PackageData', {}).get('Package', [])
        if not isinstance(packages, list):
            packages = [packages]

        # loop through the packages and apply the regex
        for package in packages:
            text = package.get('Text', '')

            # there can be multiple Media in a Package
            # for some reason the xmltodict and/or XML
            # stores as a non-list if only one element
            media_list = package.get('MediaList', {})
            if media_list:
                medias = media_list.get('Media', [])
                if not isinstance(medias, list):
                    medias = [medias]

                # add all valid images
                images = []
                for media in medias:
                    image = media.get('Image', '')
                    # TODO: not sure we need the below check
                    # since we are starting with a subset of components
                    # we know/believe to be package label info
                    if image in image_ids:
                        images.append(image)

                # check if the text matches the regex pattern
                ndc_matches = self.ndc_format(text)
                # get distinct ndc_matches because
                # sometimes the NDC is repeated multiples
                # times in a component
                if ndc_matches:
                    ndc_matches = list(set(ndc_matches))
                    # if the number of NDC maches equals
                    # the number of images
                    if len(ndc_matches) == len(images):
                        for idx, ndc_match in enumerate(ndc_matches):
                            # if ndc is valid compared to
                            # all known NDCs in SPL
                            if ndc_match in ndc_ids:
                                # map the NDC to the image in the
                                # same list position
                                # NOTE: this is an assumption and needs
                                # to be validated / verified
                                mapped_dict[ndc_match] = images[idx]

        return mapped_dict

    def extract_and_upload_mapped_ndcs_from_image_files(self):
        mapping_dict = self.file_mapping

        image_ndc_mapping = {}

        for spl, mapping in mapping_dict.items():
            # get all image file names associated with the SPL
            image_files = mapping.get('image_files')
            # get all NDCs associated with the SPL
            ndcs = mapping.get('ndcIds')
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
            # get all image file names associated with the SPL
            image_files = mapping.get('image_files')
            # get all NDCs associated with the SPL
            ndcs = mapping.get('ndcIds')

            # Get NDCs from XML components
            spl_folder_name = mapping.get("spl_folder_name")
            xml_file_path = self.get_file_path(spl_folder_name, mapping.get("xml_file"))
            xml_doc = self.find_xml_package_data(xml_file_path)
            
            matched_components = self.get_ndcs_from_image_components(xml_doc, ndcs, image_files)

            for ndc,image_file in matched_components.items():
                image_ndc_mapping[ndc] = {
                        'image_file':image_file,
                        'spl':spl, 
                        'methodology':'image_component',
                        'confidence_level':0.75,
                        'matched':1} 
            
            for ndc in ndcs:
                if ndc not in image_ndc_mapping.keys():
                    image_ndc_mapping[ndc] = {
                        'image_file':'',
                        'spl':spl, 
                        'image_url':'',
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
