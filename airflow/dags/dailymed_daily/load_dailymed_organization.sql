 /* sagerx.dailymed_organization */
 --DROP TABLE IF EXISTS sagerx.dailymed_organization CASCADE;

 CREATE TABLE IF NOT EXISTS sagerx.dailymed_organization (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	dun					TEXT,
	org_name			TEXT,
	org_type			TEXT
); 

with xml_table as
(
select spl, xml_content::xml as xml_column
from sagerx_lake.dailymed_daily
)

INSERT INTO sagerx.dailymed_organization
SELECT spl, y.*
    FROM   xml_table x,
            XMLTABLE('/dailymed/Organizations/establishment'
              PASSING xml_column
              COLUMNS 
                document_id 	TEXT  PATH '../../documentId',
				set_id  		TEXT  PATH '../../SetId',
				dun				TEXT  PATH './DUN',
	            org_name		TEXT  PATH './name',
	            org_type		TEXT  PATH './type'
					) y
ON CONFLICT DO NOTHING;