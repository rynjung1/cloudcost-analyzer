select
    usage_start_time as usage_date,
    service ->> 'description' as service_name,
    cost as cost_usd
from {{ source('raw_gcp', 'bigquery_billing_table') }}
where cost > 0
