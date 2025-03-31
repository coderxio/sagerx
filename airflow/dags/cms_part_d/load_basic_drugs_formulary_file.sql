/* sagerx_lake.cms_basic_drugs_formulary */
DROP TABLE IF EXISTS sagerx_lake.cms_basic_drugs_formulary CASCADE;

CREATE TABLE sagerx_lake.cms_basic_drugs_formulary (
formulary_id           VARCHAR(8) NOT NULL,
formulary_version         VARCHAR(5) NOT NULL,
contract_year      VARCHAR(4),
rxcui  VARCHAR(8),
ndc  VARCHAR(11),
tier_level_value    TEXT,
quantity_limit_yn    VARCHAR(1),
quantity_limit_amount      VARCHAR(7),
quantity_limit_days      VARCHAR(3),
prior_authorization_yn      VARCHAR(1),
step_therapy_yn      VARCHAR(1)
);

COPY sagerx_lake.cms_basic_drugs_formulary
FROM '{{ ti.xcom_pull(task_ids='extract') }}/basic drugs formulary file  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;
