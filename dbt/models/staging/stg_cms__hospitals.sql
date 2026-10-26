-- Current list of Medicare-registered hospitals (latest release only).
with source as (
    select * from {{ source('cms', 'hospital_general_info') }}
    where {{ latest_release(source('cms', 'hospital_general_info')) }}
)

select
    {{ to_ccn('facility_id') }}                         as ccn,
    initcap(trim(facility_name))                        as hospital_name,
    initcap(trim(city_town))                            as city,
    upper(trim(state))                                  as state,
    left(lpad(trim(zip_code), 5, '0'), 5)               as zip5,
    initcap({{ clean_text('county_parish') }})          as county,
    {{ clean_text('hospital_type') }}                   as hospital_type,
    {{ clean_text('hospital_ownership') }}              as ownership,
    coalesce(trim(emergency_services) = 'Yes', false)   as has_emergency_services,
    case when trim(hospital_overall_rating) ~ '^[1-5]$'
         then trim(hospital_overall_rating)::int end    as overall_rating,
    _release::date                                      as release_date,
    _loaded_at                                          as loaded_at
from source
