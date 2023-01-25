/* datasource.rxterms */
DROP TABLE IF EXISTS datasource.rxterms;

CREATE TABLE datasource.rxterms (
rxcui                   TEXT,
generic_rxcui           TEXT,
tty                     TEXT,
full_name               TEXT,
rxn_dose_form           TEXT,
full_generic_name       TEXT,
brand_name              TEXT,
display_name            TEXT,
route                   TEXT,
new_dose_form           TEXT,
strength                TEXT,
suppress_for            TEXT,
display_name_synonym    TEXT,
is_retired              TEXT,
sxdg_rxcui              TEXT,
sxdg_tty                TEXT,
sxdg_name               TEXT,
psn                     TEXT
);

COPY datasource.rxterms
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxterms') }}/RxTerms{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m' ) }}.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
