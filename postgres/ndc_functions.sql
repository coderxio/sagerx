--add ndc Converting functions to db public sagerx
CREATE OR REPLACE FUNCTION ndc_format(ndc CHAR(13)) 
RETURNS CHAR(8) 
LANGUAGE 'plpgsql'
AS
$$
DECLARE
	return_value CHAR(8);
BEGIN
SELECT
	CASE WHEN ndc ~ '^\d{10}$' THEN '10 Digit'
		 WHEN ndc ~ '^\d{11}$' THEN '11 Digit'
		 WHEN ndc ~ '^\d{4}-\d{4}-\d{2}$' THEN '4-4-2'
		 WHEN ndc ~ '^\d{5}-\d{3}-\d{2}$' THEN '5-3-2'
		 WHEN ndc ~ '^\d{5}-\d{4}-\d{1}$' THEN '5-4-1'
		 WHEN ndc ~ '^\d{5}-\d{4}-\d{2}$' THEN '5-4-2'
		 WHEN ndc ~ '^\d{5}-\d{5}$' THEN '5-5'
		 WHEN ndc ~ '^\d{4}-\d{6}$' THEN '4-6'
	ELSE 'Unkown'
	END
	INTO return_value;
	RETURN return_value;
END;
$$;


CREATE OR REPLACE FUNCTION ndc_to_11(ndc CHAR(13))
RETURNS CHAR(11) 
LANGUAGE 'plpgsql'
AS
$$
DECLARE
	return_value CHAR(11);
BEGIN
SELECT
	CASE WHEN ndc_format(ndc) = '10 Digit' THEN NULL
		 WHEN ndc_format(ndc) =  '11 Digit' THEN ndc
		 WHEN ndc_format(ndc) =  '4-4-2' THEN '0' || LEFT(ndc,4) || REPLACE(RIGHT(ndc,7),'-','')
		 WHEN ndc_format(ndc) =  '5-3-2' THEN LEFT(ndc,5) || '0' || REPLACE(RIGHT(ndc,6),'-','')
		 WHEN ndc_format(ndc) =  '5-4-1' THEN REPLACE(LEFT(ndc,10),'-','') || '0' || RIGHT(ndc, 1)
		 WHEN ndc_format(ndc) =  '5-4-2' THEN REPLACE(ndc,'-','')
		 WHEN ndc_format(ndc) =  '5-5' THEN NULL
		 WHEN ndc_format(ndc) =  '4-6' THEN NULL
	ELSE NULL
	END
	INTO return_value;
	RETURN return_value;
END;
$$;

CREATE OR REPLACE FUNCTION ndc_convert(ndc CHAR(13),to_format CHAR(8))
RETURNS CHAR(13)
LANGUAGE 'plpgsql'
AS
$$

DECLARE ndc11 char(11);
DECLARE format_list char(8)[];
DECLARE return_value CHAR(13);
BEGIN
	format_list := array['10 Digit','11 Digit','4-4-2','5-3-2','5-4-1','5-4-2','5-5','4-6'];
	IF NOT (to_format = ANY (format_list))
		THEN RAISE 'format must be of viable type';
	END IF;
    ndc11 := ndc_to_11(ndc);
	RETURN CASE WHEN to_format = '10 Digit' THEN NULL
		 WHEN to_format = '11 Digit' THEN ndc11
		 WHEN to_format = '4-4-2' THEN 
		 			CASE WHEN SUBSTRING(ndc11,1,1) = '0' 
							THEN SUBSTRING(ndc11,2,4) ||'-'|| SUBSTRING(ndc11,6,4) ||'-'|| RIGHT(ndc11,2)
					ELSE NULL END
		 WHEN to_format = '5-3-2' THEN 
		 			CASE WHEN SUBSTRING(ndc11,6,1) = '0' 
						THEN LEFT(ndc11,5) ||'-'|| SUBSTRING(ndc11,7,3) ||'-'|| RIGHT(ndc11,2)
					ELSE NULL END
		 WHEN to_format = '5-4-1' THEN
		 			CASE WHEN SUBSTRING(ndc11,10,1) = '0' 
						THEN LEFT(ndc11,5) ||'-'|| SUBSTRING(ndc11,6,4) ||'-'|| RIGHT(ndc11,1)
					ELSE NULL END
		 WHEN to_format = '5-4-2' THEN LEFT(ndc11,5) ||'-'|| SUBSTRING(ndc11,6,4) ||'-'|| RIGHT(ndc11,2)
		 WHEN to_format = '5-5' THEN NULL
		 WHEN to_format = '4-6' THEN NULL
	ELSE NULL
	END;
END;
$$;