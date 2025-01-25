import time
import json
import logging
import threading
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

import pendulum
import pandas as pd
from pandas import json_normalize
import re

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook

from sagerx import load_df_to_pg  

class RateLimiter:
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
    with urlopen(url) as response:
        data = json.loads(response.read())
    
    # If the body says "Too Many Requests" even though status is 200, treat it like a 429 and retry
    if isinstance(data, dict) and data.get("error") == "Too Many Requests":
        raise HTTPError(url, 429, "Too Many Requests (from response body)", None, None)
    
    return data


def process_rxcuis(base_url, rxcui, max_retries=3, initial_delay=1):
    if not rxcui:
        logging.error("process_rxcuis called for empty rxcui.")
        return None

    url = f"{base_url}{rxcui}/allhistoricalndcs.json?history=2" # Get the deepest history

    for attempt in range(max_retries):
        try:
            data_json = fetch_json(url)

            hist_concept = data_json.get("historicalNdcConcept", {})
            hist_times = hist_concept.get("historicalNdcTime", [])

            if not hist_times:
                logging.warning(f"No historicalNdcTime found for rxcui={rxcui}. Skipping.")
                return None

            rows = []
            for block in hist_times:
                status = block.get("status")
                historical_rxcui = block.get("rxcui")
                ndc_list = block.get("ndcTime", [])

                for ndc_obj in ndc_list:
                    # Grab 'ndc' from the object (could be a list or None)
                    ndc_data = ndc_obj.get("ndc")

                    # If 'ndc_data' is a non-empty list, take its first element and make it a string. There is always only one element according to the docs.
                    if isinstance(ndc_data, list) and ndc_data:
                        ndc_data = ndc_data[0]

                    # Append to the rows
                    rows.append({
                        "rxcui": rxcui,
                        "status": status,
                        "historical_rxcui": historical_rxcui,
                        "ndc": ndc_data,
                        "startDate": ndc_obj.get("startDate"),
                        "endDate": ndc_obj.get("endDate"),
                    })

            return rows

        except HTTPError as e:
            if e.code == 429:
                # Exponential backoff for rate-limit or "Too Many Requests" from body
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"Rate limit (429) for rxcui={rxcui} at {url}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                # Skip any other HTTP errors (404, 500, etc.)
                logging.error(f"HTTP error {e.code} for rxcui={rxcui}. Skipping to the next concept.")
                return None

        except URLError as e:
            # Retry for URLError as well
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"URLError for rxcui={rxcui} at {url}: {e.reason}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                logging.error(
                    f"URLError for rxcui={rxcui} at {url}: {e.reason}. "
                    f"Max retries reached. Skipping to the next concept."
                )
                return None

        except (KeyError, TypeError) as e:
            logging.error(f"Data structure error for rxcui={rxcui}: {str(e)}. Skipping to the next concept.")
            return None

        except Exception as e:
            logging.error(f"Unexpected error for rxcui={rxcui}: {str(e)}. Skipping to the next concept.")
            return None

    # If we exhausted all attempts for 429 or URLError
    logging.error(f"Max retries reached for rxcui={rxcui} at {url}. Skipping to the next concept.")
    return None


@task
def get_rxcuis() -> list:
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql( # Get rxcuis from rxnorm_rxnconso table
        """
        SELECT DISTINCT rxcui 
        FROM sagerx_lake.rxnorm_rxnconso 
        WHERE tty IN ('SCD','SBD','GPCK','BPCK') 
          AND sab = 'RXNORM'
        """,
        con=engine
    )

    rxcui_list = df['rxcui'].tolist()
    logging.info(f"Retrieved {len(rxcui_list)} RxCUIs from the DB.")
    return rxcui_list


@task
def load_historical_data(rxcuis: list):
    if not rxcuis:
        logging.warning("No RxCUIs provided.")
        return False

    logging.info(f"Starting to retrieve historical data for {len(rxcuis)} RxCUIs.")

    base_url = "https://rxnav.nlm.nih.gov/REST/rxcui/"

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        process_func = partial(process_rxcuis, base_url)
        mapped_results = executor.map(process_func, rxcuis)

        for i, result in enumerate(mapped_results, start=1):
            results.append(result)
            if i % 1000 == 0:
                logging.info(f"Processed {i} RxCUIs so far...")

    successful_rows = [
        item for sublist in results if sublist is not None for item in sublist
    ]
    failed_count = sum(1 for x in results if x is None)
    logging.info(
        f"Historical data retrieval completed. "
        f"Total RxCUIs: {len(rxcuis)}, Successful: {len(rxcuis) - failed_count}, Failed: {failed_count}"
    )

    if not successful_rows:
        logging.warning("No data retrieved. The result list is empty.")
        return False

    # Create DataFrame
    df = pd.DataFrame(successful_rows)
    if df.empty:
        logging.warning("DataFrame is empty after flattening.")
        return False

    load_df_to_pg(df, "sagerx_lake", "rxnorm_historical", "replace")
    logging.info(f"Loaded {len(df)} rows into 'rxnorm_historical' table.")

    return True

dag_id = "rxnorm_historical"

@dag(
    dag_id=dag_id,
    schedule= "0 0 1 1 *",
    max_active_runs=1,
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxnorm_historical():
    rxcui_task = get_rxcuis()
    ndc_task = load_historical_data(rxcuis=rxcui_task)

    rxcui_task >> ndc_task

dag = rxnorm_historical()
