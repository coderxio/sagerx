 /* staging.dailymed_organization_text */
 --DROP TABLE IF EXISTS staging.dailymed_organization_text CASCADE;

 CREATE TABLE IF NOT EXISTS staging.dailymed_organization_text (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	organization_text	TEXT
); 

with xml_table as
(
select slp, xml_content::xml as xml_column
from datasource.dailymed_daily
)

INSERT INTO staging.dailymed_organization_text
SELECT slp, y.*
    FROM   xml_table x,
            XMLTABLE('/dailymed/Organizations/OrganizationsText'
              PASSING xml_column
              COLUMNS 
                document_id 		TEXT PATH '../../documentId',
				set_id  			TEXT PATH '../../SetId',
				organization_text	TEXT PATH '.' 
					) y
ON CONFLICT DO NOTHING;