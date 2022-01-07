/* staging.fda_ndc_class  */
DROP TABLE IF EXISTS staging.fda_ndc_class;

CREATE TABLE staging.fda_ndc_class (
	productid	TEXT NOT NULL,
	productndc 	TEXT NOT NULL,
	class_line 	TEXT NOT NULL,
	class_name 	TEXT,
	class_type 	TEXT,
	PRIMARY KEY (productid,productndc,class_line)
);

INSERT INTO staging.fda_ndc_class
SELECT
	z.productid
	,ROW_NUMBER() OVER (PARTITION BY z.productid ORDER BY z.token DESC) AS class_line
	,LEFT(z.token, POSITION('[' IN z.token) -1 ) AS class_name
    ,substring(z.token, '\[(.+)\]') as class_type

FROM (SELECT DISTINCT t.productid
	,t.pharm_classes
	, s.token
	FROM   datasource.fda_ndc_product t, unnest(string_to_array(t.pharm_classes, ',')) s(token)) z
;

