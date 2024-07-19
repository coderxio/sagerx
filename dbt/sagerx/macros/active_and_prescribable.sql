{% macro active_and_prescribable() %}
case
    when suppress = 'N'
        then true
    else false
end as active,
case
    when cvf = '4096'
        then true
    else false
end as prescribable
{% endmacro %}
