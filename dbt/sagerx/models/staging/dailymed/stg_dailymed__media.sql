 /* staging.stg_dailymed__media */

with xml_table as
(
	select spl, xml_content::xml as xml_column
	from sagerx_lake.dailymed
)

select
	spl
	, p_label.document_id
	, p_label.set_id
	, p_label.version_number
	, p_label.text
	, y.*
from xml_table x,
	xmltable(
		'//PackageLabel' passing xml_column
		columns 
			document_id 	text  path '../../documentId',
			set_id  		text  path '../../SetId',
			version_number	text  path '../../VersionNumber',
			text			text  path 'Text',
			media_list		xml   path 'MediaList'
	) as p_label,
	xmltable(
		'MediaList/Media' passing p_label.media_list
		columns
			image			text  path 'Image',
			image_id		text  path 'ID'
	) as y
