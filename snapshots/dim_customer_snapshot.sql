{% snapshot dim_customer_snapshot %}
{{
    config(
      target_schema='mart',
      target_database=target.database,
      unique_key='customer_id',
      strategy='check',
      check_cols=[
        'first_name',
        'last_name',
        'phone',
        'email',
        'address',
        'city',
        'state',
        'zip',
        'preferred_contact_method'
      ]
    )
}}

select
    customer_id,
    first_name,
    last_name,
    phone,
    email,
    address,
    city,
    state,
    zip,
    creation_date,
    preferred_contact_method,
    dbt_loaded_at
from {{ ref('stg_customers') }}
where has_bad_customerid = FALSE
  and has_bad_email = FALSE

{% endsnapshot %}