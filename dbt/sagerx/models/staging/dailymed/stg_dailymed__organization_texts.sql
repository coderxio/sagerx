 /* staging.stg_dailymed__organization_texts */

with xml_table as
(
	select zip_file, xml_content::xml as xml_column
	from sagerx_lake.dailymed
)

select zip_file
		, document_id
		, set_id 
		, version_number
		, organization_text
		, row_num
from (select zip_file
		, y.document_id
		, y.set_id
		, y.version_number
		, y.organization_text
		--,regexp_matches(organization_text, '(manufactured|distributed) (by|for):([\s\S]*)(?=manufactured|distributed|made)', 'ig') as mfdg_by_match
		,row_number() over (partition by zip_file order by length(organization_text) desc) as row_num
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
