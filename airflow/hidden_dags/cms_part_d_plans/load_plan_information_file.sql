/* sagerx_lake.cms_plan_information */
DROP TABLE IF EXISTS sagerx_lake.cms_plan_information CASCADE;

CREATE TABLE sagerx_lake.cms_plan_information (
contract_id           VARCHAR(5) NOT NULL,
plan_id         VARCHAR(3) NOT NULL,
segment_id      VARCHAR(3),
contract_name  VARCHAR(100),
plan_name  VARCHAR(80),
formulary_id    VARCHAR(8),
premium    TEXT,
deductible      TEXT,
icl      TEXT,
ma_region_code      VARCHAR(2),
pdp_region_code      VARCHAR(2),
state       VARCHAR(2),
county_code       VARCHAR(5),
snp       VARCHAR(1),
plan_suppressed_yn       VARCHAR(1)
);

COPY sagerx_lake.cms_plan_information
FROM '{{ ti.xcom_pull(task_ids='extract') }}/plan information  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER ENCODING 'WIN1252';;