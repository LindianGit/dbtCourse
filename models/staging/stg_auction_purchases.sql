{{ 
    config(
        materialized='incremental',
        unique_key='auction_id',
        incremental_strategy='merge',
        schema='stage'                 
    )
}}

--
-- stg_auction_purchases.sql  Staging model for auction purchases
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (auction_id)
-- - Data cleaning: trims spaces, corrects case.
-- - Use of type casting for data consistency.
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table gets record updates retroactively, using AuctionDate
--       as the incremental filter may not capture late-arriving or updated data. 
--       Prefer an "updated_at" column if available.
-- schema='stage'  This sets the schema to 'stage'

with deduped_source as (
    select
        AuctionID,
        AuctionDate,
        VIN,
        PurchasePrice,
        PurchasedBy,
        AuctionHouse,
        OutletID,
        row_number() over (
            partition by AuctionID
            order by AuctionDate desc
        ) as rn
    from {{ source('raw', 'CORE_AUCTION_PURCHASES') }}
    {% if is_incremental() %}
        where TRY_TO_DATE(AuctionDate) > (select max(auction_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['AuctionID']) }} as auction_sk,  -- Surrogate key for warehouse joins, NOT NULL
    cast(AuctionID as varchar)         as auction_id,                     -- Original system auction ID, NOT NULL
    TRY_TO_DATE(AuctionDate)           as auction_date,                   -- Date of auction
    trim(VIN)                          as vin,                            -- Vehicle Identification Number
    TRY_TO_NUMBER(PurchasePrice)       as purchase_price,                 -- Purchase price, numeric type
    trim(PurchasedBy)                  as purchased_by,                   -- Name of purchaser
    trim(AuctionHouse)                 as auction_house,                  -- Name of auction house
    cast(OutletID as integer)          as outlet_id,                      -- Outlet identifier
   -- current_timestamp                  as dbt_loaded_at,                  -- Load timestamp
    TRY_TO_DATE(AUCTIONDATE) as dbt_loaded_at,          -- use as effective, not current date
    'stg_auction_purchases'            as dbt_model_name,                 -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when TRY_TO_DATE(AuctionDate) is null then TRUE
        else FALSE
    end as has_bad_auctiondate,                                           -- TRUE if auction date is null or invalid

    case 
        when AuctionID is null or AuctionID = '' then TRUE
        else FALSE
    end as has_bad_auctionid,                                             -- TRUE if missing auction id

    case
        when VIN is null or VIN = '' or len(trim(VIN)) < 6 then TRUE
        else FALSE
    end as has_bad_vin                                                    -- TRUE for null/short/invalid VIN

from deduped_source
where rn = 1 -- Only keep the latest record for each AuctionID