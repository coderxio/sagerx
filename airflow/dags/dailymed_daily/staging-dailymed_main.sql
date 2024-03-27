 /* sagerx.dailymed_main */
 DROP TABLE IF EXISTS sagerx.dailymed_main CASCADE;

 CREATE TABLE IF NOT EXISTS sagerx.dailymed_main (
	spl 				TEXT NOT NULL,
	document_id 		TEXT NOT NULL,
	set_id			 	TEXT,
	version_number 		TEXT,
   	effective_date 		TEXT,
	market_status		TEXT,
	application_number	TEXT,
	dailymed_url		TEXT
); 

with xml_table as
(
select spl, xml_content::xml as xml_column
from sagerx_lake.dailymed_daily
)

INSERT INTO sagerx.dailymed_main
SELECT spl, y.*, 'https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid=' || y.set_id
    FROM   xml_table x,
            XMLTABLE('dailymed'
              PASSING xml_column
              COLUMNS 
                document_id 	TEXT  PATH './documentId',
				set_id  		TEXT  PATH './SetId',
				version_number	TEXT  PATH './VersionNumber',
  				effective_date	TEXT  PATH './EffectiveDate',
				market_status	TEXT  PATH './MarketStatus',
				application_number TEXT PATH './ApplicationNumber'

					) y
ON CONFLICT DO NOTHING;