-- The headline table: for the latest data year, what each hospital costs for
-- each procedure group (DRG) versus its state, next to its quality.
-- Answers "where can I get good care for this procedure without paying more?"
with latest_year as (
    select max(data_year) as data_year from {{ ref('fct_inpatient_drg_payments') }}
),

payments as (
    select f.*
    from {{ ref('fct_inpatient_drg_payments') }} f
    join latest_year using (data_year)
    where f.discharges >= {{ var('min_discharges') }}
)

select
    p.data_year,
    p.ccn,
    coalesce(h.hospital_name, initcap(p.provider_name))     as hospital_name,
    h.city,
    p.state,
    h.hospital_type,
    p.drg_cd,
    p.drg_desc,
    p.discharges,
    p.avg_total_payment,
    p.avg_medicare_payment,
    p.state_median_total_payment,
    p.price_index_vs_state,
    h.overall_rating,
    h.readmission_rate_pct,
    h.mortality_worse_count,
    case
        when h.overall_rating is null                                   then 'Unrated'
        when h.overall_rating >= 4 and p.price_index_vs_state <= 1.00   then 'High value'
        when h.overall_rating >= 4                                      then 'High quality, higher cost'
        when h.overall_rating <= 2 and p.price_index_vs_state > 1.00    then 'Low value'
        when h.overall_rating <= 2                                      then 'Lower cost, lower quality'
        else 'Average'
    end                                                     as value_segment,
    rank() over (
        partition by p.state, p.drg_cd
        order by p.avg_total_payment
    )                                                       as price_rank_in_state
from payments p
left join {{ ref('dim_hospital') }} h using (ccn)
