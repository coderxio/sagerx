/* staging.stg_dailymed__organization_activities */
 
 DROP TABLE IF EXISTS staging.stg_dailymed__organization_activities CASCADE;

 CREATE TABLE staging.stg_dailymed__organization_activities (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	dun					TEXT,
	activity			TEXT
); 

WITH xml_table as
(
select spl, xml_content::xml as xml_column
from datasource.dailymed_rx_full
)

INSERT INTO staging.stg_dailymed__organization_activities
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