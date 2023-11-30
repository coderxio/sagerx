 /* staging.stg_dailymed__organization_texts */

with xml_table as
(
	select spl, xml_content::xml as xml_column
	from datasource.dailymed_rx_full
)

select spl
		, document_id
		, set_id 
		, version_number
		, organization_text
		, row_num
from (select spl
		, y.document_id
		, y.set_id
		, y.version_number
		, y.organization_text
		--,regexp_matches(organization_text, '(manufactured|distributed) (by|for):([\s\S]*)(?=manufactured|distributed|made)', 'ig') as mfdg_by_match
		,row_number() over (partition by spl order by length(organization_text) desc) as row_num
    from   xml_table x,
            xmltable('/dailymed/Organizations/OrganizationsText'
              passing xml_column
              columns 
                document_id 		text path '../../documentId',
				set_id  			text path '../../SetId',
				version_number	 text  path '../../VersionNumber',
				organization_text	text path '.' 
					) y
	) z
where row_num = 1
