{% snapshot dim_outlet_snapshot %}
{{
    config(
      target_schema='mart',
      target_database=target.database,
      unique_key='outlet_id',
      strategy='timestamp',
      updated_at='dbt_loaded_at'
    )
}}

select
    outlet_id,
    outlet_name,
    address,
    city,
    state,
    zip,
    dbt_loaded_at
from {{ ref('stg_outlets') }}
where has_bad_outletid = FALSE

{% endsnapshot %}