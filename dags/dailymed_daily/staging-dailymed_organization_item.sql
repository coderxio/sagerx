/* staging.dailymed_organization_item */
 --DROP TABLE IF EXISTS staging.dailymed_organization_item CASCADE;

 CREATE TABLE IF NOT EXISTS staging.dailymed_organization_item (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	dun					TEXT,
	activity			TEXT,
	item				TEXT
); 

WITH xml_table as
(
select spl, xml_content::xml as xml_column
from datasource.dailymed_daily
)

INSERT INTO staging.dailymed_organization_item
SELECT spl, y.*
    FROM   xml_table x,
            XMLTABLE('/dailymed/Organizations/establishment/function/item_list/item'
              PASSING xml_column
              COLUMNS 
                document_id 	TEXT  PATH '../../../../../documentId',
				set_id  		TEXT  PATH '../../../../../SetId',
				dun				TEXT  PATH '../../../DUN',
	            activity		TEXT  PATH '../../name',
				item			TEXT  PATH '.'
					) y
ON CONFLICT DO NOTHING;