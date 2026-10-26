-- Every data year loaded into raw must reach the incremental fact table with
-- the same number of rows. Catches incremental-filter bugs that silently skip a year.
with raw_years as (
    select data_year, count(*) as raw_rows
    from {{ ref('stg_cms__inpatient_drg') }}
    group by data_year
),
fact_years as (
    select data_year, count(*) as fact_rows
    from {{ ref('fct_inpatient_drg_payments') }}
    group by data_year
)
select r.data_year, r.raw_rows, f.fact_rows
from raw_years r
left join fact_years f using (data_year)
where f.fact_rows is distinct from r.raw_rows
