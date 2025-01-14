 /* sagerx_dev.stg_dailymed__ndcs */

with xml_table as
(
	select zip_file, xml_content::xml as xml_column
	from sagerx_lake.dailymed
),

sql_table as (

	select zip_file, y.*
		from   xml_table x,
				xmltable('dailymed/NDCList/NDC'
				passing xml_column
				columns 
					document_id 	text  path '../../documentId',
					set_id  		text  path '../../SetId',
					version_number	 text  path '../../VersionNumber',
					ndc				text  path '.'
						) y

),

cte as (

	select
		*,
		{{ ndc_to_11('ndc') }} as ndc11
	
	from sql_table

)

select * from cte
