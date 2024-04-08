{% macro ndc_convert (ndc, to_format) %}

    {% set ndc11 = ndc_to_11(ndc) %}
    {% set format_list = ['10 Digit','11 Digit','4-4-2','5-3-2','5-4-1','5-4-2','5-5','4-6'] %}
    {% if to_format not in format_list %}
        {{ "format must be of viable type" }}
    {% endif %}
    

    {%- set return_value %}
        CASE WHEN {{to_format}} = '10 Digit' THEN NULL
            WHEN {{to_format}} = '11 Digit' THEN {{ndc11}}
		    WHEN {{to_format}} = '4-4-2' THEN 
		 			CASE WHEN SUBSTRING({{ndc11}},1,1) = '0' THEN SUBSTRING({{ndc11}},2,4) ||'-'|| SUBSTRING({{ndc11}},6,4) ||'-'|| RIGHT({{ndc11}},2) ELSE NULL END
            WHEN {{to_format}} = '5-3-2' THEN 
		 			CASE WHEN SUBSTRING({{ndc11}},6,1) = '0' THEN LEFT({{ndc11}},5) ||'-'|| SUBSTRING({{ndc11}},7,3) ||'-'|| RIGHT({{ndc11}},2) ELSE NULL END
		    WHEN {{to_format}} = '5-4-1' THEN
		 			CASE WHEN SUBSTRING({{ndc11}},10,1) = '0' THEN LEFT({{ndc11}},5) ||'-'|| SUBSTRING({{ndc11}},6,4) ||'-'|| RIGHT({{ndc11}},1) ELSE NULL END
		    WHEN {{to_format}} = '5-4-2' THEN LEFT({{ndc11}},5) ||'-'|| SUBSTRING({{ndc11}},6,4) ||'-'|| RIGHT({{ndc11}},2)
		    WHEN {{to_format}} = '5-5' THEN NULL
		    WHEN {{to_format}} = '4-6' THEN NULL
	    ELSE NULL
	    END
  {% endset %}
{{return_value}}

{% endmacro %}
