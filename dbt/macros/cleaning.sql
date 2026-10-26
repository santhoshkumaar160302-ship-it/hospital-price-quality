{#- Helpers for CMS text columns. Raw tables store everything as TEXT. -#}

{#- Treat CMS placeholder values as NULL. -#}
{% macro clean_text(col) -%}
    nullif(nullif(nullif(trim({{ col }}), ''), 'Not Available'), 'Not Applicable')
{%- endmacro %}

{#- Numeric if it looks like a number ("$1,234.50" -> 1234.50), else NULL. -#}
{% macro to_numeric(col) -%}
    case
        when regexp_replace(trim({{ col }}), '[$,]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then regexp_replace(trim({{ col }}), '[$,]', '', 'g')::numeric
    end
{%- endmacro %}

{% macro to_int(col) -%}
    case when trim({{ col }}) ~ '^[0-9]+$' then trim({{ col }})::int end
{%- endmacro %}

{#- CMS dates are MM/DD/YYYY. -#}
{% macro to_date_mdy(col) -%}
    case when trim({{ col }}) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' then to_date(trim({{ col }}), 'MM/DD/YYYY') end
{%- endmacro %}

{#- CMS Certification Number: 6 characters. Spreadsheets often strip the
    leading zero ("50001" -> should be "050001"). -#}
{% macro to_ccn(col) -%}
    lpad(upper(trim({{ col }})), 6, '0')
{%- endmacro %}

{#- Only rows from the most recent release of a source (current state). -#}
{% macro latest_release(relation) -%}
    _release = (select max(_release) from {{ relation }})
{%- endmacro %}
