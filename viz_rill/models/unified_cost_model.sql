select
    'aws' as cloud_provider,
    cost_id,
    usage_date,
    service_name,
    cost_usd
from {{ ref('stg_aws_costs') }}

union all

select
    'azure' as cloud_provider,
    cost_id,
    usage_date,
    service_name,
    cost_usd
from {{ ref('stg_azure_costs') }}

union all

select
    'gcp' as cloud_provider,
    cost_id,
    usage_date,
    service_name,
    cost_usd
from {{ ref('stg_gcp_costs') }}
