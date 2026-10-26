-- SCD Type 2 hospital dimension built from the snapshot: one row per version.
select
    ccn,
    hospital_name,
    state,
    hospital_type,
    ownership,
    has_emergency_services,
    overall_rating,
    release_date                                       as first_seen_in_release,
    row_number() over (partition by ccn order by dbt_valid_from) as version,
    dbt_valid_from                                     as valid_from,
    dbt_valid_to                                       as valid_to,
    dbt_valid_to is null                               as is_current
from {{ ref('snap_hospitals') }}
