from airflow.decorators import task
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import logging
from functools import lru_cache
from io import StringIO
from collections import deque
from sagerx import write_json_file, read_json_file, create_path, load_df_to_pg
from common_dag_tasks import get_data_folder

logger = logging.getLogger(__name__)


class ATCScraper:
    def __init__(self):
        self.base_url = "https://www.whocc.no/atc_ddd_index/"
        self.atc_roots = ['A', 'B', 'C', 'D', 'G', 'H', 'J', 'L', 'M', 'N', 'P', 'R', 'S', 'V']
        
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    @lru_cache(maxsize=10000)
    def get_atc_level(self, code: str) -> int:
        length = len(code)
        if length == 1: return 1
        elif length == 3: return 2
        elif length == 4: return 3
        elif length == 5: return 4 # Level 4 pages contain DDD tables for Level 5 codes
        elif length == 7: return 5
        else: return -1 
    
    @lru_cache(maxsize=10000)
    def fetch_page(self, code: str) -> str:
        url = f"{self.base_url}?code={code}&showdescription=no"
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {code} at {url}: {e}")
            return ""

    def parse_subcodes(self, html: str, parent_code: str) -> pd.DataFrame:
        if not html:
            return pd.DataFrame()
            
        soup = BeautifulSoup(html, 'html.parser')
        content_p = soup.select_one("#content > p:nth-of-type(2)") 
        
        if not content_p:
            content_paragraphs = soup.select("#content p")
            if len(content_paragraphs) > 1:
                content_p = content_paragraphs[1]
            elif content_paragraphs:
                 content_p = content_paragraphs[0]
            
            if not content_p:
                return pd.DataFrame()

        text = content_p.get_text(separator='\n').strip()
        if not text:
            return pd.DataFrame()
        
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        data = []
        current_parent_level = self.get_atc_level(parent_code)

        for line in lines:
            parts = line.split(maxsplit=1)
            if not parts: continue
                
            potential_code = parts[0]
            
            if not (potential_code and potential_code[0].isalpha() and potential_code.isalnum()):
                continue

            code_level = self.get_atc_level(potential_code)
            if code_level > current_parent_level and potential_code.startswith(parent_code):
                name = parts[1].strip() if len(parts) > 1 else "N/A"
                name_parts_cleaned = []
                for word in name.split():
                    if word.replace('.', '', 1).replace(',', '', 1).isdigit() and \
                       any(char.isdigit() for char in word):
                        break 
                    name_parts_cleaned.append(word)
                name = ' '.join(name_parts_cleaned)

                data.append({
                    'atc_code': potential_code,
                    'atc_name': name,
                    'atc_level': code_level 
                })
            
        return pd.DataFrame(data)
    
    def parse_ddd_table(self, html: str, page_code: str) -> pd.DataFrame:
        if not html:
            return pd.DataFrame()

        soup = BeautifulSoup(html, 'html.parser')
        
        table = soup.select_one("#content ul li table") 
        if not table: table = soup.select_one("#content table")
        if not table: table = soup.select_one("table")

        if not table:
            return pd.DataFrame()
        
        try:
            html_string = str(table)
            dfs = pd.read_html(StringIO(html_string), header=0)
            
            if not dfs:
                logger.warning(f"Page {page_code}: pd.read_html found no tables from HTML string.")
                return pd.DataFrame()
            
            df = dfs[0]
            
            expected_cols_map = {
                'atc code': 'atc_code', 'name': 'atc_name',
                'ddd': 'ddd', 'u': 'uom', 'adm.r': 'adm_route', 'note': 'note'
            }
            df.columns = df.columns.str.lower().str.strip()
            df = df.rename(columns=expected_cols_map)

            if 'atc_code' not in df.columns:
                logger.error(f"Page {page_code}: 'atc_code' column MISSING after renaming. Cols: {df.columns.tolist()}")
                return pd.DataFrame()
            
            if 'atc_name' in df.columns:
                df['atc_name'] = df['atc_name'].ffill()
            df['atc_code'] = df['atc_code'].ffill().astype(str)

            df = df[df['atc_code'].str.len() == 7]
            if df.empty:
                return pd.DataFrame()
            
            df = df.replace('', np.nan)
            
            ddd_columns_to_check = ['ddd', 'uom', 'adm_route', 'note']
            existing_ddd_cols = [col for col in ddd_columns_to_check if col in df.columns]
            
            if existing_ddd_cols:
                df = df.dropna(subset=existing_ddd_cols, how='all')
            
            return df
            
        except Exception as e:
            logger.error(f"Page {page_code}: Error parsing DDD table: {e}", exc_info=True)
            return pd.DataFrame()
    
    def scrape_code_iterative(self, root_code: str) -> pd.DataFrame:
        all_data_for_root = []
        visited_codes = set()
        queue = deque([(root_code, 1)]) 
        max_depth = 6 

        while queue:
            current_code, depth = queue.popleft()
            
            if current_code in visited_codes: continue
            visited_codes.add(current_code)

            if depth > max_depth:
                logger.warning(f"Max depth {max_depth} reached for {current_code}, stopping branch.")
                continue
            
            try:
                html_content = self.fetch_page(current_code)
                if not html_content:
                    continue

                current_atc_level = self.get_atc_level(current_code)
                
                if current_atc_level in [1, 2, 3]:
                    subcodes_df = self.parse_subcodes(html_content, current_code)
                    if not subcodes_df.empty:
                        for _, row in subcodes_df.iterrows():
                            subcode = row['atc_code']
                            if subcode not in visited_codes:
                                queue.append((subcode, depth + 1))
                
                elif current_atc_level == 4:
                    ddd_df = self.parse_ddd_table(html_content, current_code)
                    if not ddd_df.empty:
                        all_data_for_root.append(ddd_df)

            except Exception as e:
                logger.error(f"Unhandled error scraping {current_code}: {e}", exc_info=True)
        
        return pd.concat(all_data_for_root, ignore_index=True) if all_data_for_root else pd.DataFrame()
    
    def scrape_all(self, max_workers: int = 10) -> pd.DataFrame:
        all_scraped_data = []
        
        logger.info(f"Starting parallel scrape with {max_workers} workers for {len(self.atc_roots)} roots...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_root_code = {
                executor.submit(self.scrape_code_iterative, root): root 
                for root in self.atc_roots
            }
            
            for future in as_completed(future_to_root_code):
                root_code_val = future_to_root_code[future]
                try:
                    data_from_root = future.result()
                    if data_from_root is not None and not data_from_root.empty:
                        all_scraped_data.append(data_from_root)
                        logger.info(f"Completed {root_code_val}: {len(data_from_root)} DDD entries found.")
                    else:
                        logger.info(f"Completed {root_code_val}: No DDD entries found.")
                except Exception as e:
                    logger.error(f"Error processing root {root_code_val}: {e}")
                    logger.error(f"Exception from worker for root {root_code_val}", exc_info=True)
        
        if all_scraped_data:
            logger.info("Combining and cleaning all scraped data...")
            final_df = pd.concat(all_scraped_data, ignore_index=True)
            
            essential_cols = ['atc_code', 'atc_name', 'ddd', 'uom', 'adm_route', 'note']
            for col in essential_cols:
                if col not in final_df.columns:
                    final_df[col] = np.nan
            final_df = final_df[essential_cols]

            key_cols_for_dedup = ['atc_code']
            for col in ['ddd', 'uom', 'adm_route']:
                if col in final_df.columns:
                    key_cols_for_dedup.append(col)
            
            final_df = final_df.drop_duplicates(subset=key_cols_for_dedup, keep='first')
            final_df = final_df.sort_values('atc_code').reset_index(drop=True)
            
            logger.info(f"Final combined DataFrame shape: {final_df.shape}")
            return final_df
        else:
            logger.warning("No data was scraped from any ATC root.")
            return pd.DataFrame()


@task
def extract(dag_id: str) -> str:
    logging.info("Starting ATC DDD scraping...")
    
    scraper = ATCScraper()
    atcddd_df = scraper.scrape_all(max_workers=10)
    
    # Convert DataFrame to records format for JSON storage
    results = atcddd_df.to_dict('records')
    
    data_folder = get_data_folder(dag_id)
    file_path = create_path(data_folder) / 'data.json'
    file_path_str = file_path.resolve().as_posix()
    
    write_json_file(file_path_str, results)
    
    print(f"Extraction Completed! Data saved to file: {file_path_str}")
    print(f"Total records scraped: {len(results)}")
    
    return file_path_str


@task
def load(file_path_str: str):
    results = read_json_file(file_path_str)
    
    # Convert back to DataFrame
    df = pd.DataFrame(results)
    
    print(f'DataFrame created of {len(df)} length.')
    print(f'Columns: {df.columns.tolist()}')
    
    # Load to PostgreSQL
    load_df_to_pg(df, "sagerx_lake", "atc_ddd", "replace", index=False)
    
    print(f"Successfully loaded {len(df)} records to sagerx_lake.atc_ddd")