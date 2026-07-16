select
    resource_id as cost_id,
    date::date as usage_date,
    meter_category as service_name,
    cost_in_usd::numeric as cost_usd
from {{ source('raw_azure', 'azure_costs') }}
where cost_in_usd::numeric != 0
