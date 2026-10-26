-- Current hospital dimension: attributes + summarized quality.
select
    h.ccn,
    h.hospital_name,
    h.city,
    h.state,
    h.zip5,
    h.county,
    h.hospital_type,
    h.ownership,
    h.has_emergency_services,
    h.overall_rating,
    q.readmission_rate_pct,
    q.patient_safety_index,
    coalesce(q.mortality_measures_reported, 0) as mortality_measures_reported,
    coalesce(q.mortality_worse_count, 0)       as mortality_worse_count,
    coalesce(q.measures_better_count, 0)       as measures_better_count,
    coalesce(q.measures_worse_count, 0)        as measures_worse_count,
    case
        when h.overall_rating >= 4 then 'high'
        when h.overall_rating = 3  then 'average'
        when h.overall_rating <= 2 then 'low'
        else 'unrated'
    end                                        as quality_tier,
    h.release_date                             as source_release_date
from {{ ref('stg_cms__hospitals') }} h
left join {{ ref('int_hospital_quality_scores') }} q using (ccn)
