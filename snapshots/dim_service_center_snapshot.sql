 {% snapshot dim_service_center_snapshot %}
{{
    config(
      target_schema='mart',
      target_database=target.database,
      unique_key='service_center_id',
      strategy='check',
      check_cols=[
        'service_center_name',
        'address',
        'city',
        'state',
        'zip',
        'outlet_id'
      ]
    )
}}

select
    service_center_id,
    service_center_name,
    address,
    city,
    state,
    zip,
    outlet_id,
    dbt_loaded_at
from {{ ref('stg_service_centers') }}
where has_bad_service_center_id = FALSE

{% endsnapshot %}