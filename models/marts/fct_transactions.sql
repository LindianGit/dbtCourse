{{ config(
    materialized='incremental',
    unique_key='fact_transaction_key',
    schema='mart',
    database='SAMSUNGDB',
    cluster_by=['dim_date_key', 'dim_outlet_key']
) }}

-- SALES TRANSACTIONS
with sales as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.sale_id']) }} as fact_transaction_key,
        'SALE' as transaction_type,
        d.dim_date_key,
        c.customer_id as dim_customer_key,
        e.employee_id as dim_employee_key,
        o.outlet_id as dim_outlet_key,
        v.vin as dim_inventory_key,
        null as dim_service_center_key,
        s.sale_price,
        s.payment_method,
        s.trade_in_vin,
        null as repair_id,
        null as service_type,
        null as parts_cost,
        null as labor_cost,
        null as mechanic_id,
        null as mileage_at_service,
        null as warranty_work,
        null as auction_id,
        null as purchase_price,
        null as purchased_by,
        null as auction_house,
        s.sale_date,  -- < ----- always have an event date 
        current_timestamp as load_timestamp
    from {{ ref('stg_sales') }} s
    left join {{ ref('dim_date') }} d
        on cast(s.sale_date as date) = cast(d.date_value as date)
    left join {{ ref('dim_customer_snapshot') }} c
        on s.customer_id = c.customer_id
        and s.sale_date >= c.dbt_valid_from
        and (s.sale_date < c.dbt_valid_to or c.dbt_valid_to is null)
    left join {{ ref('dim_employee_snapshot') }} e
        on s.sold_by = e.employee_id
        and s.sale_date >= e.dbt_valid_from
        and (s.sale_date < e.dbt_valid_to or e.dbt_valid_to is null)
    left join {{ ref('dim_outlet_snapshot') }} o
        on s.outlet_id = o.outlet_id
        and o.dbt_valid_to is null
    left join {{ ref('dim_vehicle_snapshot') }} v
        on s.vin = v.vin
        and s.sale_date >= v.dbt_valid_from
        and (s.sale_date < v.dbt_valid_to or v.dbt_valid_to is null)
),

-- REPAIR TRANSACTIONS
repairs as (
    select
        {{ dbt_utils.generate_surrogate_key(['r.repair_id']) }} as fact_transaction_key,
        'REPAIR' as transaction_type,
        d.dim_date_key,
        null as dim_customer_key,
        e.employee_id as dim_employee_key,
        o.outlet_id as dim_outlet_key,
        v.vin as dim_inventory_key,
        sc.service_center_id as dim_service_center_key,
        null as sale_price,
        null as payment_method,
        null as trade_in_vin,
        r.repair_id,
        r.service_type,
        r.parts_cost,
        r.labor_cost,
        r.mechanic_id,
        r.mileage_at_service,
        r.warranty_work,
        null as auction_id,
        null as purchase_price,
        null as purchased_by,
        null as auction_house,
        r.repair_date,     -- < ----- always have an event date 
        current_timestamp as load_timestamp
    from {{ ref('stg_repairs') }} r
    left join {{ ref('dim_date') }} d
        on cast(r.repair_date as date) = cast(d.date_value as date)
    left join {{ ref('dim_employee_snapshot') }} e
        on r.mechanic_id = e.employee_id
        and r.repair_date >= e.dbt_valid_from
        and (r.repair_date < e.dbt_valid_to or e.dbt_valid_to is null)
    left join {{ ref('dim_service_center_snapshot') }} sc
        on r.service_center_id = sc.service_center_id
        and sc.dbt_valid_to is null
    left join {{ ref('dim_outlet_snapshot') }} o
        on sc.outlet_id = o.outlet_id
        and o.dbt_valid_to is null
    left join {{ ref('dim_vehicle_snapshot') }} v
        on r.vin = v.vin
        and v.dbt_valid_to is null
),

-- AUCTION TRANSACTIONS
auctions as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.auction_id']) }} as fact_transaction_key,
        'AUCTION' as transaction_type,
        d.dim_date_key,
        null as dim_customer_key,
        null as dim_employee_key,
        o.outlet_id as dim_outlet_key,
        v.vin as dim_inventory_key,
        null as dim_service_center_key,
        null as sale_price,
        null as payment_method,
        null as trade_in_vin,
        null as repair_id,
        null as service_type,
        null as parts_cost,
        null as labor_cost,
        null as mechanic_id,
        null as mileage_at_service,
        null as warranty_work,
        a.auction_id,
        a.purchase_price,
        a.purchased_by,
        a.auction_house,
        a.auction_date,   -- < ----- always have an event date 
        current_timestamp as load_timestamp
    from {{ ref('stg_auction_purchases') }} a
    left join {{ ref('dim_date') }} d
        on cast(a.auction_date as date) = cast(d.date_value as date)
    left join {{ ref('dim_outlet_snapshot') }} o
        on a.outlet_id = o.outlet_id
        and o.dbt_valid_to is null   --<---- no loaded at type date possible
    left join {{ ref('dim_vehicle_snapshot') }} v
        on a.vin = v.vin
        and a.auction_date >= v.dbt_valid_from
        and (a.auction_date < v.dbt_valid_to or v.dbt_valid_to is null)
)

select * from sales
union all
select * from repairs
union all
select * from auctions