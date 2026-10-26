{#- Custom generic tests (no external packages needed). Each returns failing rows. -#}

{% test not_negative(model, column_name) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test valid_ccn(model, column_name) %}
{#- CMS Certification Number: exactly 6 alphanumeric characters. -#}
select {{ column_name }} from {{ model }}
where {{ column_name }} is not null and {{ column_name }} !~ '^[0-9A-Z]{6}$'
{% endtest %}

{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
