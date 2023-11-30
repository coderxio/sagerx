/* staging.stg_dailymed__organization_activities */
 
with xml_table as
(
	select spl, xml_content::xml as xml_column
	from datasource.dailymed_rx_full
)

select spl, y.*
    from   xml_table x,
            xmltable('/dailymed/Organizations/establishment/function'
              passing xml_column
              columns 
                document_id 	text  path '../../../documentId',
				set_id  		text  path '../../../SetId',
				version_number	 text  path '../VersionNumber',
				dun				text  path '../DUN',
	            activity		text  path './name'
					) y
