 /* staging.stg_dailymed__interactions */

with xml_table as
(
	select zip_file, xml_content::xml as xml_column
	from sagerx_lake.dailymed
)

select zip_file, y.*
    from   xml_table x,
            xmltable('dailymed/InteractionText'
              passing xml_column
              columns 
                document_id 	 text  path '../documentId',
				set_id  		 text  path '../SetId',
				version_number	 text  path '../VersionNumber',
				interaction_text text path '.'
					) y
