select
    md5(
        coalesce(usage_start_time::text, '') || '|' ||
        coalesce(project_id, '') || '|' ||
        coalesce(service_id, '') || '|' ||
        coalesce(sku_id, '')
    ) as cost_id,
    usage_start_time::date as usage_date,
    service_description as service_name,
    cost as cost_usd
from {{ source('raw_gcp', 'bigquery_billing_table') }}
where cost != 0
