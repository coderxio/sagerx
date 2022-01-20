/* flatfile.opps_asp_mapping */
CREATE OR REPLACE VIEW flatfile.opps_asp_mapping
AS
SELECT ndc.hcpcs
	,labeler_name
	,drug_name
	,ndc.short_description
	,SI
	,ndc_to_11(ndc) AS ndc
	,hcpcs_dosage
	,pkg_size
	,pkg_qty
	,billing_units
	,bill_units_pkg
	,payment_limit
	,ROUND( (payment_limit::numeric * 1.06), 2) AS reimbursement_non_340B
	,ROUND( (payment_limit::numeric * (1-.225)), 2) AS reimbursement_340B
FROM datasource.asp_ndc_hcpcs ndc
	INNER JOIN datasource.cms_asp_pricing asp ON ndc.hcpcs = asp.hcpcs
	LEFT JOIN datasource.cms_addendum_b addb ON ndc.hcpcs = addb.hcpcs