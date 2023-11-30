 /* staging.stg_dailymed__ndcs */

with xml_table as
(
	select spl, xml_content::xml as xml_column
	from datasource.dailymed_rx_full
)

select spl, y.*, ndc_to_11(y.ndc) as ndc11
    from   xml_table x,
            xmltable('dailymed/NDCList'
              passing xml_column
              columns 
                document_id 	text  path '../documentId',
				set_id  		text  path '../SetId',
				version_number	 text  path '../VersionNumber',
				ndc				text  path '.'
					) y
