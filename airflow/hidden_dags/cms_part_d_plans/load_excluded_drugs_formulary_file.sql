/* sagerx_lake.cms_excluded_drugs_formulary */
DROP TABLE IF EXISTS sagerx_lake.cms_excluded_drugs_formulary CASCADE;

CREATE TABLE sagerx_lake.cms_excluded_drugs_formulary (
contract_id           VARCHAR(5) NOT NULL,
plan_id         VARCHAR(3) NOT NULL,
rxcui      VARCHAR(8),
tier  TEXT,
quantity_limit_yn  VARCHAR(5),
quantity_limit_amount    VARCHAR(8),
quantity_limit_days    VARCHAR(3),
prior_auth_yn      VARCHAR(1),
step_therapy_yn      VARCHAR(1),
capped_benefit_yn      VARCHAR(1),
gap_cov      VARCHAR(1)
);

COPY sagerx_lake.cms_excluded_drugs_formulary
FROM '{{ ti.xcom_pull(task_ids='extract') }}/excluded drugs formulary file  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;