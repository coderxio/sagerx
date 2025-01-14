{% macro ndc_format(ndc) %}
  {% set ndc_format %}
    CASE WHEN {{ndc}} ~ '^\d{10}$' THEN '10 Digit'
        WHEN {{ndc}} ~ '^\d{11}$' THEN '11 Digit'
        WHEN {{ndc}} ~ '^\d{4}-\d{4}-\d{2}$' THEN '4-4-2'
        WHEN {{ndc}} ~ '^\d{5}-\d{3}-\d{2}$' THEN '5-3-2'
        WHEN {{ndc}} ~ '^\d{5}-\d{4}-\d{1}$' THEN '5-4-1'
        WHEN {{ndc}} ~ '^\d{5}-\d{4}-\d{2}$' THEN '5-4-2'
        WHEN {{ndc}} ~ '^\d{5}-\d{5}$' THEN '5-5'
        WHEN {{ndc}} ~ '^\d{4}-\d{6}$' THEN '4-6'
	  ELSE 'Unknown'
	  END
  {% endset %}
  {{ndc_format}}
{% endmacro %}
