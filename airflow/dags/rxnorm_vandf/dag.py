import logging
import requests
import pandas as pd
import time
import threading

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from sagerx import load_df_to_pg
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = Variable.get("umls_api")
TTY = "IN+PIN+MIN+SCDC+SCDF+SCDFP+SCDG+SCDGP+SCD+GPCK+BN+SBDC+SBDF+SBDFP+SBDG+SBD+BPCK"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                # Remove timestamps older than 'self.period'
                self.calls = [t for t in self.calls if now - t < self.period]
                # If limit is reached, sleep
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    logging.debug(f"RateLimiter: sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                self.calls.append(time.time())
            return func(*args, **kwargs)
        return wrapped

# Separate rate limiters for RxNav and UMLS
rxnav_rate_limiter = RateLimiter(max_calls=20, period=1)  # 20 calls/s for RxNav
umls_rate_limiter  = RateLimiter(max_calls=20, period=1)  # 20 calls/s for UMLS

@rxnav_rate_limiter
def fetch_rxcuis_by_tty(tty: str) -> list:
    """
    Fetch RxCUIs for a given TTY from RxNav.
    """
    url = f"https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty={tty}"
    logging.info(f"Fetching RxCUIs for TTY={tty} from RxNav: {url}")
    try:
        resp = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error connecting to RxNav: {e}")
        return []
    
    if resp.status_code != 200:
        logging.error(f"HTTP {resp.status_code} error fetching RxCUIs from RxNav.")
        return []
    
    data = resp.json()
    if "minConceptGroup" not in data or "minConcept" not in data["minConceptGroup"]:
        logging.error(f"Unexpected JSON format from RxNav: {url}")
        return []
    
    concepts = data["minConceptGroup"]["minConcept"]
    rxcuis = [c["rxcui"] for c in concepts]
    logging.info(f"Fetched {len(rxcuis)} RxCUIs for TTY={tty}.")
    return rxcuis

@rxnav_rate_limiter
def get_drug_names_for_rxcui(rxcui: str, session: requests.Session) -> list:
    """
    For a given RxCUI, fetch drug names via RxClass byRxcui endpoint.
    """
    url = f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}"
    try:
        resp = session.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error connecting to RxClass for RxCUI={rxcui}: {e}")
        return []
    
    if resp.status_code != 200:
        logging.warning(f"RxClass HTTP {resp.status_code} for RxCUI={rxcui}.")
        return []
    
    data = resp.json()
    if "rxclassDrugInfoList" not in data:
        # Possibly no classes found for this RxCUI
        return []
    
    # Extract 'drug_name' from each 'rxclassDrugInfo' -> 'minConcept' -> 'name'
    drug_names = []
    for info in data["rxclassDrugInfoList"].get("rxclassDrugInfo", []):
        min_concept = info.get("minConcept", {})
        name = min_concept.get("name")
        if name:
            drug_names.append(name)
    
    # Return unique names
    return list(set(drug_names))

@umls_rate_limiter
def crosswalk_rxcui_to_vandf(rxcui: str) -> list:
    """
    Crosswalk an RxCUI to VANDF codes using the UMLS crosswalk endpoint.
    """
    base_uri = "https://uts-ws.nlm.nih.gov"
    endpoint = f"/rest/crosswalk/current/source/RXNORM/{rxcui}"
    params = {
        "targetSource": "VANDF",
        "apiKey": API_KEY
    }
    url = f"{base_uri}{endpoint}"
    
    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error connecting to UMLS crosswalk for RxCUI={rxcui}: {e}")
        return []
    
    if resp.status_code != 200:
        logging.warning(f"UMLS crosswalk HTTP {resp.status_code} for RxCUI={rxcui}.")
        return []
    
    data = resp.json()
    results = data.get("result", [])
    out_list = []
    
    for item in results:
        vandf_code = item.get("ui", "")
        vandf_name = item.get("name", "")
        root_source = item.get("rootSource", "")
        out_list.append({
            "rxcui": rxcui,
            "vandf_code": vandf_code,
            "vandf_name": vandf_name,
            "root_source": root_source
        })
    
    return out_list


def process_rxcui(rxcui: str, session: requests.Session) -> list:
    rows = []
    drug_names = get_drug_names_for_rxcui(rxcui, session)
    vandf_entries = crosswalk_rxcui_to_vandf(rxcui)
    if not vandf_entries:
        return rows

    if drug_names:
        for v in vandf_entries:
            for dn in drug_names:
                row = {
                    "rxcui": v["rxcui"],
                    "drug_name": dn,
                    "vandf_code": v["vandf_code"],
                    "vandf_name": v["vandf_name"],
                    "root_source": v["root_source"]
                }
                rows.append(row)
    else:
        for v in vandf_entries:
            row = {
                "rxcui": v["rxcui"],
                "drug_name": None,
                "vandf_code": v["vandf_code"],
                "vandf_name": v["vandf_name"],
                "root_source": v["root_source"]
            }
            rows.append(row)
    return rows


def build_rxcui_vandf_crosswalk(tty=TTY) -> pd.DataFrame:
    session = requests.Session()
    
    # 1. Fetch RxCUIs
    all_rxcuis = fetch_rxcuis_by_tty(tty)
    total_rxcuis = len(all_rxcuis)
    if total_rxcuis == 0:
        logging.warning("No RxCUIs found. Returning empty DataFrame.")
        return pd.DataFrame()

    all_rows = []
    
    # 2. Process each RxCUI in parallel
    max_workers = 10
    logging.info(f"Processing {total_rxcuis} RxCUIs in parallel with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_rxcui, rxcui, session): rxcui
            for rxcui in all_rxcuis
        }
        
        for i, future in enumerate(as_completed(futures), start=1):
            rxcui = futures[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as e:
                logging.error(f"Error processing RxCUI={rxcui}: {e}")
            
            # Log progress every 100 completions
            if i % 100 == 0 or i == total_rxcuis:
                logging.info(f"Processed {i}/{total_rxcuis} RxCUIs so far...")

    # 3. Create a DataFrame from all the collected rows
    df = pd.DataFrame(all_rows)
    if df.empty:
        logging.info("No VANDF crosswalk rows found (empty DataFrame).")
        return df

    df.drop_duplicates(inplace=True)
    logging.info(f"Completed building crosswalk DataFrame with {len(df)} rows.")
    return df


@task
def main_execution():
    """
    Main Airflow task: Build the RxCUI -> VANDF crosswalk, then load it into Postgres
    """
    logging.info("Starting RxCUI -> VANDF crosswalk build...")

    # Build the crosswalk DataFrame
    crosswalk_df = build_rxcui_vandf_crosswalk(tty=TTY)
    if crosswalk_df.empty:
        logging.warning("Crosswalk DataFrame is empty. Nothing to load.")
        return False
    
    # Load into Postgres
    logging.info("Loading crosswalk DataFrame into Postgres table 'rxnorm_vandf'...")
    load_df_to_pg(crosswalk_df, "sagerx_lake", "rxnorm_vandf", "replace")
    logging.info("Done.")
    return True

dag_id = "rxnorm_vandf"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxnorm_vandf_dag():

    start = EmptyOperator(task_id="start")

    build_and_load_task = main_execution()

    end = EmptyOperator(task_id="end")

    start >> build_and_load_task >> end

dag = rxnorm_vandf_dag()
