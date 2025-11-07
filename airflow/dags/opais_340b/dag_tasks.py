import logging
import pandas as pd
from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from sagerx import load_df_to_pg, free_text_to_snake


logger = logging.getLogger(__name__)


def _clean_sheet(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [free_text_to_snake(col) for col in cleaned.columns]
    cleaned = cleaned.loc[:, [col for col in cleaned.columns if col and not col.startswith("unnamed")]]
    cleaned = cleaned.dropna(how="all")
    return cleaned.reset_index(drop=True)


def _get_latest_excel(data_folder: Path) -> Path:
    excel_files = list(data_folder.glob("*.xlsx"))

    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {data_folder}")

    return max(excel_files, key=lambda p: p.stat().st_mtime)


def _load_cleaned_sheet(sheet_name: str, raw_df: pd.DataFrame, table: str) -> None:
    logger.info("Cleaning sheet '%s' (%d rows)", sheet_name, len(raw_df))
    df = _clean_sheet(raw_df)
    rows = len(df)
    logger.info("Sheet '%s' cleaned to %d rows", sheet_name, rows)

    if df.empty:
        logger.warning("Sheet '%s' is empty after cleaning. Skipping %s.", sheet_name, table)
        return

    load_df_to_pg(df, "sagerx_lake", table, "replace", index=False)
    logger.info("Loaded %d rows from sheet '%s' into %s", rows, sheet_name, table)


@task
def load(dag_id: str, data_folder: str) -> None:
    sheet_to_table = {
        "Covered Entities": "opais_340b_covered_entities",
        "Shipping Addresses": "opais_340b_shipping_addresses",
        "Contract Pharmacies": "opais_340b_contract_pharmacies",
    }

    latest_file = _get_latest_excel(Path(data_folder))
    logger.info("Starting OPAIS load from file: %s", latest_file)
    sheet_names = list(sheet_to_table.keys())
    logger.info("Reading workbook with sheets: %s", ", ".join(sheet_names))
    sheet_frames = pd.read_excel(
        latest_file,
        sheet_name=sheet_names,
        header=3,
        dtype=str,
        engine="openpyxl",
    )
    logger.info("Completed workbook read")

    max_workers = min(3, len(sheet_to_table))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_load_cleaned_sheet, sheet, sheet_frames[sheet], table): (sheet, table)
            for sheet, table in sheet_to_table.items()
        }

        for future in as_completed(future_map):
            try:
                future.result()
            except Exception:
                sheet, table = future_map[future]
                logger.exception("Sheet '%s' failed to load into %s", sheet, table)
                raise

    logger.info("Completed OPAIS load")
