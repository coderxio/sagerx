-- stg_fda_enforcement__reports.sql

select
	status
	, city
	, state
	, country
	, classification
	, openfda
	, product_type
	, event_id
	, recalling_firm
	, address_1
	, address_2
	, postal_code
	, voluntary_mandated
	, initial_firm_notification
	, distribution_pattern
	, recall_number
	, product_description
	, product_quantity
	, reason_for_recall
	, recall_initiation_date
	, center_classification_date
	, report_date
	, code_info
from sagerx_lake.fda_enforcement
