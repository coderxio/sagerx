import requests
import pandas as pd
import base64
import concurrent.futures
from xml.etree import ElementTree as ET

from common_dag_tasks import get_umls_ticket
from airflow.models import Variable
from airflow.decorators import task
from sagerx import load_df_to_pg

# constants
api_key = Variable.get("umls_api")

# function to retrieve tag values for a given tag name
def get_tag_values(tag_name):
    credentials = f"apikey:{api_key}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/xml"
    }
    try:
        response = requests.get(f"https://vsac.nlm.nih.gov/vsac/tagName/{tag_name}/tagValues", headers=headers)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for tag {tag_name}: {e}")
        return None
    
def parse_xml_for_tag_values(xml_content):
    root = ET.fromstring(xml_content)
    values = [tag_value.text for tag_value in root.findall('.//value')]
    return values

def get_latest_version_cms_eMeasureID(values):
    latest_versions = {}
    for value in values:
        measure_id = ''.join(filter(str.isalpha, value))
        version_number = ''.join(filter(str.isdigit, value))
        if measure_id not in latest_versions or latest_versions[measure_id] < version_number:
            latest_versions[measure_id] = version_number
    return [measure_id + 'v' + version for measure_id, version in latest_versions.items()]

def get_described_value_set_ids(tag_name, tag_value):
    credentials = f"apikey:{api_key}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/xml"
    }
    response = requests.get(f"https://vsac.nlm.nih.gov/vsac/svs/RetrieveMultipleValueSets?tagName={tag_name}&tagValue={tag_value}", headers=headers)
    root = ET.fromstring(response.text)
    value_set_ids = [value_set.get('ID') for value_set in root.findall('.//ns0:DescribedValueSet', namespaces={'ns0': 'urn:ihe:iti:svs:2008'})]
    return value_set_ids

# function to process each tag value using multithreading
def process_tag_values(tag_name, tag_values_list):
    described_value_set_ids = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ids = {executor.submit(get_described_value_set_ids, tag_name, value): value for value in tag_values_list}
        for future in concurrent.futures.as_completed(future_to_ids):
            described_value_set_ids.extend(future.result())
    return described_value_set_ids

processed_oids = set()  # to keep track of processed OIDs and avoid infinite loops

# use to get descendant codes for value sets that only provide references instead of codes
def get_descendants(self, source, code):
    ticket = get_umls_ticket()
    url = f"{self.SERVICE}/rest/content/current/source/{source}/{code}/descendants?ticket={ticket}"
    response = requests.get(url, headers={"User-Agent": "python"})
    if response.status_code != 200:
        print(f"Error fetching descendants: {response.status_code} - {response.text}")
        return []
    return [result['ui'] for result in response.json()['result']]


# use to retrieve a value set from VSAC
def retrieve_value_set(oid):
    credentials = f"apikey:{api_key}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/fhir+json"
    }
    response = requests.get(f"https://cts.nlm.nih.gov/fhir/ValueSet/{oid}", headers=headers)
    return response.json()

# convert the JSON response to a dataframe
def json_to_dataframe(response_json, current_oid=None, parent_oid=None):
    data = []

    # mapping of system URIs to recognizable names
    system_map = {
        "http://snomed.info/sct": "SNOMED",
        "http://hl7.org/fhir/sid/icd-10-cm": "ICD10CM",
        "http://hl7.org/fhir/sid/icd-9-cm": "ICD9CM",
        "http://loinc.org": "LOINC",
        "http://www.ama-assn.org/go/cpt": "CPT",
        "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets": "HCPCS",
        "http://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx": "CVX",
        "http://www.nlm.nih.gov/research/umls/rxnorm": "RXNORM",
        "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender": "HL7 AdministrativeGender",
        "http://www.cms.gov/Medicare/Coding/ICD10": "ICD10PCS",
        "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/HospitalAcqCond/Coding": "CMS Present of Admission (POA) Indicator",
        "http://www.nlm.nih.gov/research/umls/hcpcs": "HCPCS",
        "https://www.cdc.gov/nhsn/cdaportal/terminology/codesystem/hsloc.html": "HSLOC",
        "http://hl7.org/fhir/sid/cvx": "CVX",
        "http://www.ada.org/cdt": "CDT",
        "https://nahdo.org/sopt": "Source of Payment Typology (SOPT)",
        "urn:oid:2.16.840.1.113883.6.238": "CDC Race and Ethnicity"
    }

    concepts = response_json.get('compose', {}).get('include', [])
    for concept in concepts:
        system_name = system_map.get(concept.get('system', ''), "Unknown System")
        concept_codes = concept.get('concept', [])
        for code in concept_codes:
            data.append({
                "valueSetName": response_json["name"],
                "code": code["code"],
                "display": code["display"],
                "system": system_name,
                "status": response_json["status"],
                "version": response_json["version"],
                "lastUpdated": response_json["meta"]["lastUpdated"],
                "oid": current_oid if current_oid else response_json["id"], 
                "parent_oid": parent_oid if parent_oid else None 
            })

        # handle the 'descendantOf' filter for value sets that only provide references instead of codes
        filters = response_json.get('compose', {}).get('include', [{}])[0].get('filter', [])
        for filter_ in filters:
            if filter_["op"] == "descendantOf":
                descendants = get_descendants(filter_["system"], filter_["value"])
                for descendant_code in descendants:
                    data.append({
                        "valueSetName": response_json["name"],
                        "code": descendant_code,
                        "display": "",  # display is empty because the UMLS API doesn't return it
                        "system": filter_["system"],
                        "status": response_json["status"],
                        "oid": current_oid if current_oid else response_json["id"],
                        "parent_oid": parent_oid if parent_oid else None,
                        "version": response_json["version"],
                        "lastUpdated": response_json["meta"]["lastUpdated"]
                    })

        # if the value set references other value sets, retrieve those as well
        referenced_value_sets = concept.get('valueSet', [])
        for ref_vs in referenced_value_sets:
            # extract OID from the reference URL
            oid = ref_vs.split('/')[-1]
            if oid not in processed_oids:  # avoid re-processing already processed OIDs
                data.extend(json_to_dataframe(retrieve_value_set(oid), current_oid=oid, parent_oid=current_oid))

    return data

# concurrent function that retrieves and processes OIDs
def retrieve_and_process(oid):
    processed_oids.add(oid)
    response_json = retrieve_value_set(oid)
    return json_to_dataframe(response_json, current_oid=oid)


# main execution
@task
def main_execution():
    tag_names = ['CMS eMeasure ID', 'eMeasure Identifier', 'NQF Number']
    tag_values = {}
    for tag_name in tag_names:
        xml_response = get_tag_values(tag_name)
        if xml_response:
            tag_values[tag_name] = xml_response
    print('got tag_names')

    described_value_set_ids = []
    for tag_name, xml_content in tag_values.items():
        tag_values_list = parse_xml_for_tag_values(xml_content)

        if tag_name == 'CMS eMeasure ID':
            tag_values_list = get_latest_version_cms_eMeasureID(tag_values_list)

        ids = process_tag_values(tag_name, tag_values_list)
        described_value_set_ids.extend(ids)
    print('got described_value_set_ids')

    # use ThreadPoolExecutor for concurrent requests
    all_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(retrieve_and_process, oid) for oid in described_value_set_ids]
        for future in concurrent.futures.as_completed(futures):
            all_data.extend(future.result())
            if len(all_data) % 100 == 0:
                print(f'MILESTONE {len(all_data)}')

    result_df = pd.DataFrame(all_data)

    # reset the index of the final dataframe and remove duplicates
    result_df = result_df.drop_duplicates(subset=['valueSetName', 'code', 'display'])

    load_df_to_pg(result_df, "sagerx_lake", "vsac", "replace")
    