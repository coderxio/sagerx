import re
import xmltodict
import pandas as pd

def parse_xml_content(row:pd.Series):
    from airflow.exceptions import AirflowException

    xml_string = row['raw_xml_content'] 
    xml_data = xmltodict.parse(xml_string)
    if 'document' not in xml_data.keys():
        raise AirflowException("Unexpected XML Data, expected DailyMed Document")
    
    return xml_data

def clean_string(input_string):
    cleaned_string = input_string.strip()
    cleaned_string = ' '.join(cleaned_string.split())
    return cleaned_string

def validate_ndc(ndc):
    raw_ndc = ndc.replace("-","")
    if len(raw_ndc) > 9:
        return ndc

def find_image_ids(xml_doc, results=None):
    if results is None:
        results = []
        
    if isinstance(xml_doc, dict):
        if '@mediaType' in xml_doc and xml_doc['@mediaType'] == 'image/jpeg':
            results.append(xml_doc.get('reference', {}).get('@value'))
        for _, value in xml_doc.items():
            if isinstance(value, (dict, list)):
                find_image_ids(value, results)
    elif isinstance(xml_doc, list):
        for item in xml_doc:
            find_image_ids(item, results)
    return list(set(results))

def find_ndc_numbers(xml_doc, results=None) -> list:
    target_code_system = '2.16.840.1.113883.6.69'
    if results is None:
        results = []
        
    if isinstance(xml_doc, dict):
        if '@codeSystem' in xml_doc and xml_doc['@codeSystem'] == target_code_system: 
            ndc = xml_doc.get('@code')
            if ndc and validate_ndc(ndc):
                results.append(ndc)
        for _, value in xml_doc.items():
            if isinstance(value, (dict, list)):
                find_ndc_numbers(value, results)
    elif isinstance(xml_doc, list):
        for item in xml_doc:
            find_ndc_numbers(item,results)
    return list(set(results))


def extract_set_id(xml_doc):
    return xml_doc['document']['setId']['@root']


def ndc_format(text_data):
    # order of patterns is important, largest to smalles
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

def find_image_components(xml_doc):
    components = []
    for component in xml_doc['document']['component']['structuredBody']['component']:
        if component['section']['code']['@code'] == '51945-4':
            components.append(component)
    return components

def find_ndc_in_image_component(component, results=None):
    if results is None:
        results = []

    if isinstance(component, str) and ndc_format(component):
        results.append(ndc_format(component))
    elif isinstance(component, dict):
        if '#text' in component and ndc_format(component['#text']):
            results.append(ndc_format(component['#text']))
        for _, value in component.items():
                if isinstance(value, (dict, list)):
                    find_ndc_in_image_component(value, results)
    elif isinstance(component, list):
        for item in component:
            find_ndc_in_image_component(item,results)
    return list(set(results))

def create_dailymed_image_url(image_id, set_id):
    return f"https://dailymed.nlm.nih.gov/dailymed/image.cfm?name={image_id}&setid={set_id}&type=img"

def convert_ndc_10_to_11(ndc):
    parts = ndc.split('-')
    if len(parts[-1]) == 1:
        parts[-1] = '0' + parts[-1]
    return '-'.join(parts)

def map_ndcs_from_image_ids(row:pd.Series):
    set_id = row['set_id']
    ndc_ids = row['ndc_ids']
    image_ids = row['image_ids']
    med_dict = {}

    converted_ndc_ids = [convert_ndc_10_to_11(ndc) for ndc in ndc_ids]

    for id in image_ids:
        image_ndc = ndc_format(id)
        if image_ndc and (image_ndc in converted_ndc_ids):
            med_dict[image_ndc] = id
        # else:
        #     print(f"NDC {image_ndc} from {id} not found in NDC list of: {converted_ndc_ids}")
    return med_dict

def map_ndcs_from_image_components(row:pd.Series):
    xml_doc = row['raw_xml_content'] 
    set_id = row['set_id']
    ndc_ids = row['ndc_ids']
    image_ids = row['image_ids']
    med_dict = {}

    image_components = find_image_components(xml_doc)
    
    if image_components == []:
        return None 
    
    for component in image_components:
        ndcs = find_ndc_in_image_component(component)
        images = find_image_ids(component)

        if not ndcs or not images:
            continue

        elif len(ndcs) == 1 and len(images) == 1:
            ndc = ndcs[0]
            image = images[0]

            if ndc in ndc_ids and image in image_ids:
                med_dict[ndc] = image
        #     else:
        #         print(f"Found unknown ndc or image in set ID: {set_id}")
        #         print(f"NDCs expected: {ndc_ids}")
        #         print(f"NDC got: {ndc}")
        #         print(f"Images expected: {image_ids}")
        #         print(f"Image got: {image}")
        # else:
        #     print(f"Multiple NDC - Image mapping found for set ID: {set_id}")
        #     print(f"NDCs got: {ndcs}")
        #     print(f"Images got: {images}")
            
    return med_dict

def map_ndcs_parent_function(row:pd.Series): 
    med_dict = map_ndcs_from_image_ids(row)

    if not med_dict:
        med_dict = map_ndcs_from_image_components(row)

    return med_dict