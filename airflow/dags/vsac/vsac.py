import requests
import pandas as pd
import base64
import concurrent.futures
from xml.etree import ElementTree as ET
from tqdm import tqdm

# Constants
BASE_URL_SVS = "https://cts.nlm.nih.gov/fhir"
API_KEY = ''

# Function to retrieve tag values for a given tag name
def get_tag_values(tag_name):
    credentials = f"apikey:{API_KEY}".encode('utf-8')
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
        tqdm.write(f"Error fetching data for tag {tag_name}: {e}")
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
    credentials = f"apikey:{API_KEY}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/xml"
    }
    response = requests.get(f"https://vsac.nlm.nih.gov/vsac/svs/RetrieveMultipleValueSets?tagName={tag_name}&tagValue={tag_value}", headers=headers)
    root = ET.fromstring(response.text)
    value_set_ids = [value_set.get('ID') for value_set in root.findall('.//ns0:DescribedValueSet', namespaces={'ns0': 'urn:ihe:iti:svs:2008'})]
    return value_set_ids

# Function to process each tag value using multithreading
def process_tag_values(tag_name, tag_values_list):
    described_value_set_ids = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ids = {executor.submit(get_described_value_set_ids, tag_name, value): value for value in tag_values_list}
        for future in tqdm(concurrent.futures.as_completed(future_to_ids), total=len(tag_values_list), desc=f"Processing tag values for {tag_name}"):
            described_value_set_ids.extend(future.result())
    return described_value_set_ids

# Main execution
tag_names = ['CMS eMeasure ID', 'eMeasure Identifier', 'NQF Number']
tag_values = {}
for tag_name in tqdm(tag_names, desc="Retrieving tag values"):
    xml_response = get_tag_values(tag_name)
    if xml_response:
        tag_values[tag_name] = xml_response

described_value_set_ids = []
for tag_name, xml_content in tag_values.items():
    tag_values_list = parse_xml_for_tag_values(xml_content)

    if tag_name == 'CMS eMeasure ID':
        tag_values_list = get_latest_version_cms_eMeasureID(tag_values_list)

    ids = process_tag_values(tag_name, tag_values_list)
    described_value_set_ids.extend(ids)

processed_oids = set()  # To keep track of processed OIDs and avoid infinite loops

class UMLSFetcher:
    def __init__(self, api_key):
        self.API_KEY = api_key
        self.SERVICE = "https://uts-ws.nlm.nih.gov"
        self.TICKET_GRANTING_TICKET = self.get_ticket_granting_ticket()

    def get_ticket_granting_ticket(self):
        params = {'apikey': self.API_KEY}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent":"python" }
        response = requests.post("https://utslogin.nlm.nih.gov/cas/v1/api-key", headers=headers, data=params)
        response.raise_for_status()
        return response.url.split('/')[-1]

    def get_service_ticket(self):
        params = {'service': self.SERVICE}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent":"python" }
        response = requests.post(f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{self.TICKET_GRANTING_TICKET}", headers=headers, data=params)
        response.raise_for_status()
        return response.text

    # Use to get descendant codes for value sets that only provide references instead of codes
    def get_descendants(self, source, code):
        ticket = self.get_service_ticket()
        url = f"{self.SERVICE}/rest/content/current/source/{source}/{code}/descendants?ticket={ticket}"
        response = requests.get(url, headers={"User-Agent": "python"})
        if response.status_code != 200:
            tqdm.write(f"Error fetching descendants: {response.status_code} - {response.text}")
            return []
        return [result['ui'] for result in tqdm(response.json()['result'], desc=f"Retrieving descendants for {code}")]

umls_fetcher = UMLSFetcher(API_KEY)

# Use to retrieve a value set from VSAC
def retrieve_value_set(oid):
    credentials = f"apikey:{API_KEY}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/fhir+json"
    }
    response = requests.get(f"{BASE_URL_SVS}/ValueSet/{oid}", headers=headers)
    return response.json()

# Convert the JSON response to a DataFrame
def json_to_dataframe(response_json, current_oid=None, parent_oid=None):
    data = []

    # Mapping of system URIs to recognizable names
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

        # Handle the 'descendantOf' filter for value sets that only provide references instead of codes
        filters = response_json.get('compose', {}).get('include', [{}])[0].get('filter', [])
        for filter_ in filters:
            if filter_["op"] == "descendantOf":
                descendants = umls_fetcher.get_descendants(filter_["system"], filter_["value"])
                for descendant_code in descendants:
                    data.append({
                        "valueSetName": response_json["name"],
                        "code": descendant_code,
                        "display": "",  # Display is empty because the UMLS API doesn't return it
                        "system": filter_["system"],
                        "status": response_json["status"],
                        "oid": current_oid if current_oid else response_json["id"],
                        "parent_oid": parent_oid if parent_oid else None,
                        "version": response_json["version"],
                        "lastUpdated": response_json["meta"]["lastUpdated"]
                    })

        # If the value set references other value sets, retrieve those as well
        referenced_value_sets = concept.get('valueSet', [])
        for ref_vs in referenced_value_sets:
            # Extract OID from the reference URL
            oid = ref_vs.split('/')[-1]
            if oid not in processed_oids:  # Avoid re-processing already processed OIDs
                data.extend(json_to_dataframe(retrieve_value_set(oid), current_oid=oid, parent_oid=current_oid))

    return data

# Concurrent function that retrieves and processes OIDs
def retrieve_and_process(oid):
    processed_oids.add(oid)
    response_json = retrieve_value_set(oid)
    return json_to_dataframe(response_json, current_oid=oid)

# Use ThreadPoolExecutor for concurrent requests with tqdm progress bar
all_data = []
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(retrieve_and_process, oid) for oid in described_value_set_ids]
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(described_value_set_ids), desc="Processing OIDs"):
        all_data.extend(future.result())

result_df = pd.DataFrame(all_data)

# Reset the index of the final dataframe and remove duplicates
result_df = result_df.drop_duplicates(subset=['valueSetName', 'code', 'display'])

result_df
