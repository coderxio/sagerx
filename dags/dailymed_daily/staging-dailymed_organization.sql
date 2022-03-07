 /* staging.dailymed_organization */
 --DROP TABLE IF EXISTS staging.dailymed_organization CASCADE;

 CREATE TABLE IF NOT EXISTS staging.dailymed_organization (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	dun					TEXT,
	org_name			TEXT,
	org_type			TEXT
); 

with xml_table as
(
select slp, xml_content::xml as xml_column
from datasource.dailymed_daily
)

INSERT INTO staging.dailymed_organization
SELECT slp, y.*
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