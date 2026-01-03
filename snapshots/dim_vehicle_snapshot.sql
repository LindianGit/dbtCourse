 {% snapshot dim_vehicle_snapshot %}
{{
  config(
    target_schema='mart',
    target_database=target.database,
    unique_key='vin',
    strategy='check',
    check_cols=[
      'car_make',
      'car_model',
      'year',
      'color',
      'initial_mileage',
      'acquisition_source',
      'acquisition_date',
      'acquisition_cost',
      'current_status',
      'outlet_id'
    ]
  )
}}
select
    vin,
    car_make,
    car_model,
    year,
    color,
    initial_mileage,
    acquisition_source,
    acquisition_date,
    acquisition_cost,
    current_status,
    outlet_id,
    dbt_loaded_at
from {{ ref('stg_inventory') }}
where vin is not null
{% endsnapshot %}