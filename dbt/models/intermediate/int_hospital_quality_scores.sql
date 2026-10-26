-- One row per hospital summarizing the quality measures used in the marts.
with measures as (
    select
        *,
        case
            when measure_id like 'MORT%'                 then 'mortality'
            when measure_id like 'PSI%' or measure_id like 'COMP%' then 'safety'
            when measure_id like 'READM%' or measure_id like 'EDAC%'
              or measure_id = 'HYBRID_HWR'               then 'readmission'
            else 'other'
        end                                              as measure_category,
        case
            when compared_to_national like 'Better%'     then 'better'
            when compared_to_national like 'Worse%'      then 'worse'
            when compared_to_national like 'No Different%' then 'no_different'
        end                                              as vs_national
    from {{ ref('stg_cms__quality_measures') }}
)

select
    ccn,
    max(score) filter (
        where measure_id in ({{ "'" ~ var('hospital_wide_readmission_measures') | join("', '") ~ "'" }})
    )                                                                   as readmission_rate_pct,
    max(score) filter (where measure_id = 'PSI_90')                     as patient_safety_index,
    count(*) filter (where measure_category = 'mortality' and score is not null) as mortality_measures_reported,
    count(*) filter (where measure_category = 'mortality' and vs_national = 'better') as mortality_better_count,
    count(*) filter (where measure_category = 'mortality' and vs_national = 'worse')  as mortality_worse_count,
    count(*) filter (where vs_national = 'better')                      as measures_better_count,
    count(*) filter (where vs_national = 'worse')                       as measures_worse_count,
    count(*) filter (where score is not null)                           as measures_reported
from measures
group by ccn
