/* sagerx_lake.cms_beneficiary_cost */
DROP TABLE IF EXISTS sagerx_lake.cms_beneficiary_cost CASCADE;

CREATE TABLE sagerx_lake.cms_beneficiary_cost (
contract_id           VARCHAR(5) NOT NULL,
plan_id           VARCHAR(3) NOT NULL,
segment_id         VARCHAR(3) NOT NULL,
coverage_level      SMALLINT,
tier  SMALLINT,
days_supply  SMALLINT,

cost_type_pref  SMALLINT,
cost_amt_pref  NUMERIC(14,2),
cost_min_amt_pref  VARCHAR(12),
cost_max_amt_pref  NUMERIC(14,2),

cost_type_nonpref   SMALLINT,
cost_amt_nonpref  NUMERIC(14,2),
cost_min_amt_nonpref  VARCHAR(12),
cost_max_amt_nonpref  NUMERIC(14,2),

cost_type_mail_pref  SMALLINT,
cost_amt_mail_pref  NUMERIC(14,2),
cost_min_amt_mail_pref  VARCHAR(12),
cost_max_amt_mail_pref  NUMERIC(14,2),

cost_type_mail_nonpref   VARCHAR(1),
cost_amt_mail_nonpref  NUMERIC(14,2),
cost_min_amt_mail_nonpref  VARCHAR(12),
cost_max_amt_mail_nonpref  NUMERIC(14,2),

tier_specialty_yn   VARCHAR(1),
ded_applies_yn   VARCHAR(1),
gap_cov_tier   VARCHAR(1)
);

COPY sagerx_lake.cms_beneficiary_cost
FROM '{{ ti.xcom_pull(task_ids='extract') }}/beneficiary cost file  PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;
