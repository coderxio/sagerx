/* flatfile.cms_all */
CREATE OR REPLACE VIEW flatfile.cms_all
AS
SELECT z.source_type
	,z.hcpcs
	,z.short_description
	,z.labeler_name
	,ndc_to_11(z.ndc) AS ndc
	,z.drug_name
	,z.hcpcs_dosage
	,z.pkg_size
	,z.pkg_qty
	,z.billing_units
	,z.bill_units_pkg
	,asp_p.payment_limit AS asp_payment_limit
	,noc_p.payment_limit AS noc_payment_limit
	,COALESCE(add_b.si,'NOC') AS si
	,add_b.payment_rate

FROM (SELECT 'ASP' AS source_type
		,*
	FROM datasource.asp_ndc_hcpcs

	UNION

	SELECT 'AWP' AS source_type
		,*
	FROM datasource.awp_ndc_hcpcs

	UNION

	SELECT 'OPPS' AS source_type
		,*
	FROM datasource.opps_ndc_hcpcs
	 
	 UNION
	 
	 SELECT 'NOC' AS source_type
	  	,'' AS HCPCS
		,*
	 FROM datasource.noc_ndc_hcpcs) z
	
LEFT JOIN datasource.cms_asp_pricing asp_p ON z.hcpcs = asp_p.hcpcs
LEFT JOIN datasource.cms_noc_pricing noc_p ON noc_p.generic_name = z.short_description

LEFT JOIN datasource.cms_addendum_b add_b ON add_b.hcpcs = z.hcpcs