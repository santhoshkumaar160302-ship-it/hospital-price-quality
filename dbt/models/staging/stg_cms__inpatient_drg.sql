-- Medicare inpatient payments per hospital per DRG (procedure group), all data years.
select
    _release::int                                   as data_year,
    {{ to_ccn('rndrng_prvdr_ccn') }}                as ccn,
    trim(rndrng_prvdr_org_name)                     as provider_name,
    upper(trim(rndrng_prvdr_state_abrvtn))          as state,
    lpad(trim(drg_cd), 3, '0')                      as drg_cd,
    trim(drg_desc)                                  as drg_desc,
    {{ to_int('tot_dschrgs') }}                     as discharges,
    {{ to_numeric('avg_submtd_cvrd_chrg') }}        as avg_covered_charges,
    {{ to_numeric('avg_tot_pymt_amt') }}            as avg_total_payment,
    {{ to_numeric('avg_mdcr_pymt_amt') }}           as avg_medicare_payment,
    _loaded_at                                      as loaded_at
from {{ source('cms', 'medicare_inpatient_drg') }}
where _release ~ '^[0-9]{4}$'
