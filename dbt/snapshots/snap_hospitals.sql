{% snapshot snap_hospitals %}
{#- SCD Type 2 history of hospital attributes. CMS only publishes the current
    star rating; each quarterly release overwrites the last. Snapshotting
    after every load keeps the history: when a rating or ownership changes,
    the old row is closed (dbt_valid_to) and a new one opened. -#}
{{
    config(
        unique_key='ccn',
        strategy='check',
        check_cols=['overall_rating', 'hospital_type', 'ownership', 'has_emergency_services'],
        invalidate_hard_deletes=True,
    )
}}
select ccn, hospital_name, state, hospital_type, ownership, has_emergency_services,
       overall_rating, release_date
from {{ ref('stg_cms__hospitals') }}
{% endsnapshot %}
