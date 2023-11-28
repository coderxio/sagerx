/* staging.dailymed_organization_activity */
 --DROP TABLE IF EXISTS staging.dailymed_organization_activity CASCADE;

 CREATE TABLE IF NOT EXISTS staging.dailymed_organization_activity (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	dun					TEXT,
	activity			TEXT
); 

WITH xml_table as
(
select spl, xml_content::xml as xml_column
from datasource.dailymed_daily
)

INSERT INTO staging.dailymed_organization_activity
SELECT spl, y.*
    FROM   xml_table x,
            XMLTABLE('/dailymed/Organizations/establishment/function'
              PASSING xml_column
              COLUMNS 
                document_id 	TEXT  PATH '../../../documentId',
				set_id  		TEXT  PATH '../../../SetId',
				dun				TEXT  PATH '../DUN',
	            activity		TEXT  PATH './name'
					) y
ON CONFLICT DO NOTHING;