-- How much the price of the same procedure varies between hospitals in a state.
-- e.g. "In CA, heart failure care ranges from $X to $Y: a 2.1x spread."
select
    data_year,
    state,
    drg_cd,
    max(drg_desc)                                                        as drg_desc,
    count(*)                                                             as hospitals,
    sum(discharges)                                                      as discharges,
    round(min(avg_total_payment), 2)                                     as min_payment,
    round((percentile_cont(0.25) within group (order by avg_total_payment))::numeric, 2) as p25_payment,
    round((percentile_cont(0.50) within group (order by avg_total_payment))::numeric, 2) as median_payment,
    round((percentile_cont(0.75) within group (order by avg_total_payment))::numeric, 2) as p75_payment,
    round(max(avg_total_payment), 2)                                     as max_payment,
    round(max(avg_total_payment) / nullif(min(avg_total_payment), 0), 2) as max_to_min_ratio,
    round(sum(avg_total_payment * discharges) / nullif(sum(discharges), 0), 2) as discharge_weighted_avg_payment
from {{ ref('fct_inpatient_drg_payments') }}
where discharges >= {{ var('min_discharges') }}
group by data_year, state, drg_cd
having count(*) >= 2
