 /* sagerx.dailymed_ndc */
 --DROP TABLE IF EXISTS sagerx.dailymed_ndc CASCADE;

 CREATE TABLE IF NOT EXISTS sagerx.dailymed_ndc (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	ndc 				TEXT,
   	ndc11 				TEXT
); 

with xml_table as
(
select spl, xml_content::xml as xml_column
from sagerx_lake.dailymed_rx_full
)

INSERT INTO sagerx.dailymed_ndc
SELECT spl, y.*,{{ ndc_to_11('y.ndc')}} AS ndc11
    FROM   xml_table x,
            XMLTABLE('dailymed/ndc_list/NDC'
              PASSING xml_column
              COLUMNS 
                document_id 	TEXT  PATH '../../documentId',
				set_id  		TEXT  PATH '../../SetId',
				ndc				TEXT  PATH '.'
					) y
ON CONFLICT DO NOTHING;