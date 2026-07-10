with cloud_costs as (

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

),

daily_revenue as (

    select
        transaction_date::date as revenue_date,
        sum(amount_usd) as total_revenue_usd
    from {{ ref('stg_stripe_revenue') }}
    group by 1

)

select
    c.cloud_provider,
    c.cost_id,
    c.usage_date,
    c.service_name,
    c.cost_usd,
    r.total_revenue_usd
from cloud_costs c
left join daily_revenue r
    on c.usage_date = r.revenue_date
