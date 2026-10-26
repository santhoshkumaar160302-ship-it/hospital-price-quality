{{
    config(
        materialized='incremental',
        unique_key=['data_year', 'ccn', 'drg_cd'],
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['data_year', 'drg_cd', 'state']}, {'columns': ['ccn']}],
    )
}}
-- Medicare inpatient payments per hospital, DRG and year, with price
-- benchmarks against the state and national median for the same DRG.
--
-- Incremental: each run only (re)processes data years that are new or were
-- reloaded since the last run. A reloaded year is replaced (delete+insert),
-- never duplicated. Benchmarks are computed within a year, so processing one
-- year at a time gives the same result as a full rebuild.

with payments as (
    select * from {{ ref('stg_cms__inpatient_drg') }}
    {% if is_incremental() %}
    where data_year in (
        select distinct data_year from {{ ref('stg_cms__inpatient_drg') }}
        where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    )
    {% endif %}
),

state_benchmarks as (
    select
        data_year, state, drg_cd,
        percentile_cont(0.5) within group (order by avg_total_payment) as state_median_total_payment,
        count(*)                                                     as state_hospital_count
    from payments
    group by data_year, state, drg_cd
),

national_benchmarks as (
    select
        data_year, drg_cd,
        percentile_cont(0.5) within group (order by avg_total_payment) as national_median_total_payment
    from payments
    group by data_year, drg_cd
)

select
    p.data_year,
    p.ccn,
    p.provider_name,
    p.state,
    p.drg_cd,
    p.drg_desc,
    p.discharges,
    p.avg_covered_charges,
    p.avg_total_payment,
    p.avg_medicare_payment,
    p.avg_total_payment - p.avg_medicare_payment                         as avg_non_medicare_payment,
    round(p.avg_covered_charges / nullif(p.avg_total_payment, 0), 2)     as charge_to_payment_ratio,
    round(s.state_median_total_payment::numeric, 2)                      as state_median_total_payment,
    round(n.national_median_total_payment::numeric, 2)                   as national_median_total_payment,
    round(p.avg_total_payment / nullif(s.state_median_total_payment::numeric, 0), 3) as price_index_vs_state,
    round(p.avg_total_payment / nullif(n.national_median_total_payment::numeric, 0), 3) as price_index_vs_national,
    s.state_hospital_count,
    p.loaded_at
from payments p
join state_benchmarks s using (data_year, state, drg_cd)
join national_benchmarks n using (data_year, drg_cd)
