/* sagerx_lake.rxterms */
DROP TABLE IF EXISTS sagerx_lake.rxterms;

CREATE TABLE sagerx_lake.rxterms (
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

COPY sagerx_lake.rxterms
FROM '{data_path}/RxTerms{mnth}.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
