select
    id as transaction_id,
    to_timestamp(created) as transaction_date,
    amount / 100.0 as amount_usd,
    fee / 100.0 as fee_usd,
    net / 100.0 as net_usd,
    currency,
    status,
    type as transaction_type
from {{ source('raw_stripe', 'stripe_revenue') }}
where status = 'available' or status = 'pending'
