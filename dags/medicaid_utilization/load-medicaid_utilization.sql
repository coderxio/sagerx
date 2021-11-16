/* datasource.medicaid_utilization */
DROP TABLE IF EXISTS datasource.medicaid_utilization;

CREATE TABLE datasource.medicaid_utilization (
utilization_type                TEXT,
state                           TEXT,
ndc                             TEXT,
labeler_code                    TEXT,
product_code                    TEXT,
package_size                    TEXT,
year                            TEXT,
quarter                         TEXT,
suppression_used                TEXT,
product_name                    TEXT,
units_reimbursed                TEXT,
number_of_prescriptions         TEXT,
total_amount_reimbursed         TEXT,
medicaid_amount_reimbursed      TEXT,
non_medicaid_amount_reimbursed  TEXT
);

COPY datasource.medicaid_utilization
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_medicaid_utilization') }}'
CSV HEADER;