 /* staging.dailymed_organization_text */
 --DROP TABLE IF EXISTS staging.dailymed_organization_text CASCADE;

 CREATE TABLE IF NOT EXISTS staging.dailymed_organization_text (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	organization_text	TEXT,
	row_num				TEXT
); 

with xml_table as
(
select slp, xml_content::xml as xml_column
from datasource.dailymed_daily
)

INSERT INTO staging.dailymed_organization_text
SELECT slp
		,document_id
		,set_id 
		,organization_text
		,row_num
FROM (SELECT slp
		,y.document_id
		,y.set_id 
		,y.organization_text
		--,regexp_matches(organization_text, '(manufactured|distributed) (by|for):([\s\S]*)(?=manufactured|distributed|made)', 'ig') as mfdg_by_match
		,ROW_NUMBER() OVER (PARTITION BY slp ORDER BY LENGTH(organization_text) DESC) AS row_num
    FROM   xml_table x,
            XMLTABLE('/dailymed/Organizations/OrganizationsText'
              PASSING xml_column
              COLUMNS 
                document_id 		TEXT PATH '../../documentId',
				set_id  			TEXT PATH '../../SetId',
				organization_text	TEXT PATH '.' 
					) y
	) z
WHERE row_num = 1
ON CONFLICT DO NOTHING;