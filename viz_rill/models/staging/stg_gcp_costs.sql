select
    md5(
        coalesce(usage_start_time::text, '') || '|' ||
        coalesce(project ->> 'id', '') || '|' ||
        coalesce(service ->> 'id', '') || '|' ||
        coalesce(sku ->> 'id', '') || '|' ||
        coalesce(cost::text, '')
    ) as cost_id,
    usage_start_time as usage_date,
    service ->> 'description' as service_name,
    cost as cost_usd
from {{ source('raw_gcp', 'bigquery_billing_table') }}
where cost > 0
