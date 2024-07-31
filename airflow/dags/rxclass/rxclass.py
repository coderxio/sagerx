from urllib.request import urlopen
import pandas as pd
import json
from pandas import json_normalize
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tqdm import tqdm
import time
from urllib.error import HTTPError
import threading
import logging

# Logging
logging.basicConfig(filename='concept_processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Rate limiter class
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
                self.calls = [c for c in self.calls if now - c < self.period]
                if len(self.calls) >= self.max_calls:
                    time.sleep(self.period - (now - self.calls[0]))
                self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapped

# Apply rate limiter to fetch_json
@RateLimiter(max_calls=20, period=1)  # API rate limit is 20 calls per second
def fetch_json(url):
    with urlopen(url) as response:
        return json.loads(response.read())

def process_concept(class_base_url, concept, max_retries=3, initial_delay=1):
    url = class_base_url + concept['rxcui']
    for attempt in range(max_retries):
        try:
            cur_json = fetch_json(url)
            class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
            return [dict(concept, class_data=k['rxclassMinConceptItem']) for k in class_data]
        except HTTPError as e:
            if e.code == 429:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Rate limit hit for {concept['rxcui']}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                logging.error(f"HTTP error {e.code} for {concept['rxcui']}. Retrying in {initial_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(initial_delay)
        except Exception as e:
            logging.error(f"Error processing {concept['rxcui']}: {str(e)}. Retrying in {initial_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(initial_delay)
    
    logging.error(f"Max retries reached for {concept['rxcui']}. Skipping.")
    return None  # Return None for failed concepts

# Main execution
base_url = "https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty=IN+PIN+MIN+SCDC+SCDF+SCDFP+SCDG+SCDGP+SCD+GPCK+BN+SBDC+SBDF+SBDFP+SBDG+SBD+BPCK" # All ttys
class_base_url = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="

cui_json = fetch_json(base_url)
concepts = cui_json['minConceptGroup']['minConcept']

with ThreadPoolExecutor(max_workers=20) as executor:
    process_func = partial(process_concept, class_base_url)
    results = list(tqdm(executor.map(process_func, concepts), total=len(concepts), desc="Processing concepts"))

# Record successful and failed concepts
successful_results = [item for sublist in results if sublist is not None for item in sublist]
failed_concepts = [concept for concept, result in zip(concepts, results) if result is None]

data = successful_results

df = pd.DataFrame(data)
c_df = json_normalize(df['class_data'])
full_df = pd.concat([df.drop(columns=['class_data']), c_df], axis=1).drop_duplicates().reset_index(drop=True)

# Summary of processing
total_concepts = len(concepts)
processed_concepts = len(set(df['rxcui']))
failed_count = len(failed_concepts)
logging.info(f"Processing complete. Total concepts: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}")

# Log failed concepts
if failed_concepts:
    logging.info("Failed concepts:")
    for concept in failed_concepts:
        logging.info(f"  - RxCUI: {concept['rxcui']}, Name: {concept['name']}")

# Print summary to command line
print(f"Processing complete. Total concepts: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}")
if failed_concepts:
    print("Failed concepts:")
    for concept in failed_concepts[:10]:
        print(f"  - RxCUI: {concept['rxcui']}, Name: {concept['name']}")
    if len(failed_concepts) > 10:
        print(f"  ... and {len(failed_concepts) - 10} more. See log file for complete list.")

full_df
