-- Medicare's share can't exceed the total payment (total = Medicare + patient + third party).
select data_year, ccn, drg_cd, avg_medicare_payment, avg_total_payment
from {{ ref('fct_inpatient_drg_payments') }}
where avg_medicare_payment > avg_total_payment + 0.01
