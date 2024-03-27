/* sagerx_lake.cms_partial_gap_coverage */
DROP TABLE IF EXISTS sagerx_lake.cms_partial_gap_coverage CASCADE;

CREATE TABLE sagerx_lake.cms_partial_gap_coverage (
contract_id           VARCHAR(5) NOT NULL,
plan_id         VARCHAR(3) NOT NULL,
formulary_id      VARCHAR(8),
rxcui  VARCHAR(8),
contract_year  VARCHAR(8)
);

COPY sagerx_lake.cms_partial_gap_coverage
FROM '{{ ti.xcom_pull(task_ids='extract') }}/partial gap coverage file  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;
