{%- macro ndc_to_11(ndc) %}
  {%- set return_value %}
  CASE WHEN {{ ndc_format (ndc) }} = '10 Digit' THEN NULL
     WHEN {{ ndc_format (ndc) }} =  '11 Digit' THEN {{ndc}}
     WHEN {{ ndc_format (ndc) }} =  '4-4-2' THEN '0' || LEFT({{ndc}},4) || REPLACE(RIGHT({{ndc}},7),'-','')
     WHEN {{ ndc_format (ndc) }} =  '5-3-2' THEN LEFT({{ndc}},5) || '0' || REPLACE(RIGHT({{ndc}},6),'-','')
     WHEN {{ ndc_format (ndc) }} =  '5-4-1' THEN REPLACE(LEFT({{ndc}},10),'-','') || '0' || RIGHT({{ndc}}, 1)
     WHEN {{ ndc_format (ndc) }} =  '5-4-2' THEN REPLACE({{ndc}},'-','')
     WHEN {{ ndc_format (ndc) }} =  '5-5' THEN NULL
     WHEN {{ ndc_format (ndc) }} =  '4-6' THEN NULL
  ELSE NULL
  END
  {% endset %}
  {{return_value}}
{% endmacro -%}
