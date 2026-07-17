with validated as (

    select
        resource_id as cost_id,
        date::date as usage_date,
        meter_category as service_name,
        -- CostInUSD comes from raw CSV text; guard the cast so a malformed
        -- value (e.g. "N/A") can't crash the whole model at query time.
        case when cost_in_usd ~ '^-?[0-9]+(\.[0-9]+)?$'
            then cost_in_usd::numeric
        end as cost_usd
    from {{ source('raw_azure', 'azure_costs') }}

)

select *
from validated
where cost_usd is not null
  and cost_usd != 0
