-- National year-over-year change in what Medicare-covered care costs, per DRG.
with yearly as (
    select
        data_year,
        drg_cd,
        max(drg_desc)                                                         as drg_desc,
        sum(discharges)                                                       as discharges,
        round(sum(avg_total_payment * discharges) / nullif(sum(discharges), 0), 2) as weighted_avg_total_payment,
        round(sum(avg_medicare_payment * discharges) / nullif(sum(discharges), 0), 2) as weighted_avg_medicare_payment
    from {{ ref('fct_inpatient_drg_payments') }}
    group by data_year, drg_cd
)

select
    *,
    lag(weighted_avg_total_payment) over (partition by drg_cd order by data_year) as prior_year_payment,
    round(
        100.0 * (weighted_avg_total_payment
                 / nullif(lag(weighted_avg_total_payment) over (partition by drg_cd order by data_year), 0) - 1),
        2
    )                                                                             as yoy_change_pct
from yearly
