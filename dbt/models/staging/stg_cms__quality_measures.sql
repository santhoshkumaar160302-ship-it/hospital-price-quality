-- Hospital quality measures from two CMS datasets with the same layout,
-- latest release of each. One row per hospital per measure.
{% set sources = [
    ('unplanned_hospital_visits', 'unplanned_visits'),
    ('complications_deaths', 'complications_deaths'),
] %}

{% for table, label in sources %}
select
    {{ to_ccn('facility_id') }}                   as ccn,
    upper(trim(measure_id))                       as measure_id,
    trim(measure_name)                            as measure_name,
    '{{ label }}'                                 as source_dataset,
    {{ clean_text('compared_to_national') }}      as compared_to_national,
    {{ to_int('denominator') }}                   as denominator,
    {{ to_numeric('score') }}                     as score,
    {{ to_numeric('lower_estimate') }}            as lower_estimate,
    {{ to_numeric('higher_estimate') }}           as higher_estimate,
    {{ to_date_mdy('start_date') }}               as period_start,
    {{ to_date_mdy('end_date') }}                 as period_end,
    _release::date                                as release_date
from {{ source('cms', table) }}
where {{ latest_release(source('cms', table)) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
