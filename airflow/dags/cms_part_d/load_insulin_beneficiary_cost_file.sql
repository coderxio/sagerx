/* sagerx_lake.cms_insulin_beneficiary_cost */
DROP TABLE IF EXISTS sagerx_lake.cms_insulin_beneficiary_cost CASCADE;

CREATE TABLE sagerx_lake.cms_insulin_beneficiary_cost (
contract_id           TEXT NOT NULL,
plan_id         TEXT NOT NULL,
segment_id      TEXT,
tier            TEXT,
days_supply     TEXT,
copay_amt_pref_insln    TEXT,
copay_amt_nonpref_insln TEXT,
copay_amt_mail_pref_insln   TEXT,
copay_amt_mail_nonpref_insln    TEXT
);

COPY sagerx_lake.cms_insulin_beneficiary_cost
FROM '{{ ti.xcom_pull(task_ids='extract') }}/insulin beneficiary cost file  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;