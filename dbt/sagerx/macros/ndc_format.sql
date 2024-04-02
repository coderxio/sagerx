{% macro ndc_format(ndc) -%}
  {% set return_value = 
    CASE
      WHEN re.match('^\d{10}$', ndc) THEN '10 Digit'
      WHEN re.match('^\d{11}$', ndc) THEN '11 Digit'
      WHEN re.match('^\d{4}-\d{4}-\d{2}$', ndc) THEN '4-4-2'
      WHEN re.match('^\d{5}-\d{3}-\d{2}$', ndc) THEN '5-3-2'
      WHEN re.match('^\d{5}-\d{4}-\d{1}$', ndc) THEN '5-4-1'
      WHEN re.match('^\d{5}-\d{4}-\d{2}$', ndc) THEN '5-4-2'
      WHEN re.match('^\d{5}-\d{5}$', ndc) THEN '5-5'
      WHEN re.match('^\d{4}-\d{6}$', ndc) THEN '4-6'
      ELSE 'Unknown'
    END 
  %}
  {{ return_value }}
{%- endmacro %}
