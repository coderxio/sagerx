/* staging.fda_enforcement */
CREATE TABLE IF NOT EXISTS staging.fda_enforcement (
	status    					TEXT,
	city    					TEXT,
	state    					TEXT,
	country    					TEXT,
	classification  			TEXT,
	openfda    					TEXT,
	product_type    			TEXT,
	event_id    				TEXT,
	recalling_firm    			TEXT,
	address_1    				TEXT,
	address_2    				TEXT,
	postal_code    				TEXT,
	voluntary_mandated    		TEXT,
	initial_firm_notification   TEXT,
	distribution_pattern    	TEXT,
	recall_number    			TEXT,
	product_description    		TEXT,
	product_quantity    		TEXT,
	reason_for_recall    		TEXT,
	recall_initiation_date    	TEXT,
	center_classification_date  TEXT,
	report_date    				TEXT,
	code_info    				TEXT,
	PRIMARY KEY(recall_number)
);

INSERT INTO staging.fda_enforcement
SELECT
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
FROM datasource.fda_enforcement
ON CONFLICT DO NOTHING
;