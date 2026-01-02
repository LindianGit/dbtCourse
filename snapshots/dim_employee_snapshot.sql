{% snapshot dim_employee_snapshot %}
{{
    config(
      target_schema='mart',
      unique_key='employee_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'role', 'outlet_id', 'hire_date', 'termination_date', 'email', 'phone']
    )
}}

select
    employee_id,
    first_name,
    last_name,
    role,
    outlet_id,
    hire_date,
    termination_date,
    email,
    phone
from {{ ref('stg_employees') }}

{% endsnapshot %}