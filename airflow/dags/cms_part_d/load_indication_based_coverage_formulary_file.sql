/* sagerx_lake.cms_indication_based_coverage_formulary */
DROP TABLE IF EXISTS sagerx_lake.cms_indication_based_coverage_formulary CASCADE;

CREATE TABLE sagerx_lake.cms_indication_based_coverage_formulary (
contract_id           VARCHAR(5) NOT NULL,
plan_id         VARCHAR(3) NOT NULL,
rxcui      VARCHAR(8),
disease  VARCHAR(100)
);

COPY sagerx_lake.cms_indication_based_coverage_formulary
FROM '{{ ti.xcom_pull(task_ids='extract') }}/Indication Based Coverage Formulary File  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;
