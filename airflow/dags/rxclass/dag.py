import time
import json
import logging
import threading
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from urllib.error import HTTPError

import pendulum
import pandas as pd
from pandas import json_normalize

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from sagerx import load_df_to_pg

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RateLimiter:
    """
    Restrict calls to a maximum of `max_calls` within `period` seconds.
    Ensures we don't exceed API rate limits.
    """
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                # Remove calls that fell outside the time window
                self.calls = [c for c in self.calls if now - c < self.period]
                # If we've reached the limit, sleep until we're allowed to call again
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
                    time.sleep(sleep_time)
                self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapped


@RateLimiter(max_calls=20, period=1)  # Limit to 20 calls/second
def fetch_json(url):
    """
    Fetch JSON from a URL, respecting the rate limit.
    """
    with urlopen(url) as response:
        return json.loads(response.read())


def process_concept(class_base_url, concept, max_retries=3, initial_delay=1):
    """
    For a given concept dict that includes 'rxcui', call the rxclass API
    to retrieve class info. Implements retry logic to handle HTTP errors.
    """
    url = class_base_url + concept['rxcui']
    for attempt in range(max_retries):
        try:
            cur_json = fetch_json(url)
            class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
            # Return a list of merged data dicts
            return [
                dict(
                    concept,
                    class_data=k['rxclassMinConceptItem'],
                    rela=k.get('rela'),
                    relaSource=k.get('relaSource')
                )
                for k in class_data
            ]

        except HTTPError as e:
            if e.code == 429:
                # Exponential backoff for rate-limit errors
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"Rate limit hit for {concept['rxcui']}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                # Skip for other HTTP errors
                logging.error(
                    f"HTTP error {e.code} for {concept['rxcui']}. Will skip to the next concept."
                )
                return None

        except Exception as e:
            # Skip for any non-HTTPError exceptions
            logging.error(
                f"Error processing {concept['rxcui']}: {str(e)}. Will skip to the next concept."
            )
            return None

    # If we exhaust all retries, return None (concept failed)
    logging.error(f"Max retries reached for {concept['rxcui']}. Skipping to the next concept.")
    return None


@task
def main_execution():
    """
    Retrieves RxClass concepts from RxNav for the EPC class type,
    processes them concurrently, and loads results into Postgres.
    """
    logging.info("Starting data retrieval for RxClass...")

    # Base URLs
    base_url = (
        "https://rxnav.nlm.nih.gov/REST/allconcepts.json?"
        "tty=IN+PIN+MIN+SCDC+SCDF+SCDFP+SCDG+SCDGP+SCD+GPCK+BN+SBDC"
        "+SBDF+SBDFP+SBDG+SBD+BPCK"
    )
    class_base_url = (
        "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="
    )

    # 1. Fetch the list of concepts
    cui_json = fetch_json(base_url)
    concepts = cui_json["minConceptGroup"]["minConcept"]
    total_concepts = len(concepts)
    logging.info(f"Fetched {total_concepts} concepts from RxNav.")

    # 2. Process concepts concurrently
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        process_func = partial(process_concept, class_base_url)
        mapped_results = executor.map(process_func, concepts)

        for i, result in enumerate(mapped_results, start=1):
            results.append(result)
            if i % 1000 == 0:  # Log every 1000 concepts
                logging.info(f"Processed {i} concepts so far...")

    # 3. Flatten the list and track failures
    successful_results = [
        item for sublist in results if sublist is not None for item in sublist
    ]
    failed_concepts = [
        concept for concept, result in zip(concepts, results) if result is None
    ]

    # 4. Create DataFrame
    df = pd.DataFrame(successful_results)
    if df.empty:
        logging.warning("No data retrieved. DataFrame is empty.")
        return False  # Might raise an exception if you'd prefer to fail

    # 5. Normalize class data
    c_df = json_normalize(df["class_data"])
    full_df = (
        pd.concat([df.drop(columns=["class_data"]), c_df], axis=1)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 6. Log summary
    processed_concepts = len(set(df["rxcui"]))
    failed_count = len(failed_concepts)
    logging.info(
        "Processing complete. "
        f"Total: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}"
    )

    # 9. Load data to Postgres
    logging.info("Loading data to Postgres")
    load_df_to_pg(full_df, "sagerx_lake", "rxclass", "replace")
    logging.info(f"Successfully loaded {len(full_df)} rows into 'rxclass' table.")

    logging.info("Done.")
    return True

dag_id = "rxclass"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxclass():
    process_task = main_execution()

# Instantiate the DAG
dag = rxclass()
