 /* staging.stg_dailymed__package_label_sections */

with xml_table as
(
	select zip_file, xml_content::xml as xml_column
	from sagerx_lake.dailymed
)

select
	zip_file
	, y.*
from xml_table x,
	xmltable(
		'//PackageLabel' passing xml_column
		columns 
			document_id 	text  path '../../documentId',
			set_id  		text  path '../../SetId',
			version_number	text  path '../../VersionNumber',
			id				text  path 'ID',
			text			text  path 'Text',
			media_list		xml   path 'MediaList'
	) y
