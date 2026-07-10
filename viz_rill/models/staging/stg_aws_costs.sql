select
    identity_line_item_id as cost_id,
    line_item_usage_start_date as usage_date,
    line_item_product_code as service_name,
    line_item_unblended_cost as cost_usd,
    line_item_usage_account_id as account_id
from {{ source('raw_aws', 'aws_costs') }}
where line_item_unblended_cost > 0
