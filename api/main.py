"""
FastAPI layer exposing the unified_cost_model (built by dbt) as a REST API.

Routes:
- GET /costs                    costs for a specific cloud and month
- GET /costs/summary             total spend per cloud for the current month
- GET /costs/top-services         top N most expensive services for a cloud
- GET /costs/compare              AWS vs Azure vs GCP side by side for a month
- GET /revenue                   Stripe revenue for a month
- GET /unit-economics             cloud cost as % of Stripe revenue for a month
"""

from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from api.cache import get_cached, set_cached
from api.database import run_query

app = FastAPI(title="Cloud Cost Analyzer API")


# ---------- Pydantic response models ----------
# these define the exact shape every response is guaranteed to match

class CostRecord(BaseModel):
    cloud_provider: str
    cost_id: str
    usage_date: date
    service_name: str
    cost_usd: float


class CloudSummary(BaseModel):
    cloud_provider: str
    total_cost_usd: float


class ServiceCost(BaseModel):
    service_name: str
    total_cost_usd: float


class CloudComparison(BaseModel):
    aws: float
    azure: float
    gcp: float


class RevenueResponse(BaseModel):
    month: str
    total_revenue_usd: float


class UnitEconomics(BaseModel):
    month: str
    total_cost_usd: float
    total_revenue_usd: float
    cost_as_pct_of_revenue: Optional[float]


# ---------- Routes ----------

@app.get("/costs", response_model=list[CostRecord])
def get_costs(
    cloud: str = Query(..., description="Cloud provider: aws, azure, or gcp"),
    month: str = Query(..., description="Month in YYYY-MM format"),
):
    rows = run_query(
        """
        select cloud_provider, cost_id, usage_date, service_name, cost_usd
        from analytics.unified_cost_model
        where cloud_provider = %s
          and to_char(usage_date, 'YYYY-MM') = %s
        order by usage_date
        """,
        (cloud, month),
    )
    return rows


@app.get("/costs/summary", response_model=list[CloudSummary])
def get_cost_summary():
    cache_key = "costs:summary"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    rows = run_query(
        """
        select cloud_provider, sum(cost_usd) as total_cost_usd
        from analytics.unified_cost_model
        where date_trunc('month', usage_date) = date_trunc('month', current_date)
        group by cloud_provider
        order by total_cost_usd desc
        """
    )
    set_cached(cache_key, rows)
    return rows


@app.get("/costs/top-services", response_model=list[ServiceCost])
def get_top_services(
    cloud: str = Query(..., description="Cloud provider: aws, azure, or gcp"),
    limit: int = Query(10, ge=1, le=100),
):
    rows = run_query(
        """
        select service_name, sum(cost_usd) as total_cost_usd
        from analytics.unified_cost_model
        where cloud_provider = %s
        group by service_name
        order by total_cost_usd desc
        limit %s
        """,
        (cloud, limit),
    )
    return rows


@app.get("/costs/compare", response_model=CloudComparison)
def compare_clouds(month: str = Query(..., description="Month in YYYY-MM format")):
    cache_key = f"costs:compare:{month}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    rows = run_query(
        """
        select cloud_provider, sum(cost_usd) as total_cost_usd
        from analytics.unified_cost_model
        where to_char(usage_date, 'YYYY-MM') = %s
        group by cloud_provider
        """,
        (month,),
    )
    totals = {"aws": 0.0, "azure": 0.0, "gcp": 0.0}
    for row in rows:
        totals[row["cloud_provider"]] = float(row["total_cost_usd"])

    set_cached(cache_key, totals)
    return totals


@app.get("/revenue", response_model=RevenueResponse)
def get_revenue(month: str = Query(..., description="Month in YYYY-MM format")):
    rows = run_query(
        """
        select coalesce(sum(amount_usd), 0) as total_revenue_usd
        from analytics.stg_stripe_revenue
        where to_char(transaction_date, 'YYYY-MM') = %s
        """,
        (month,),
    )
    return {"month": month, "total_revenue_usd": float(rows[0]["total_revenue_usd"])}


@app.get("/unit-economics", response_model=UnitEconomics)
def get_unit_economics(month: str = Query(..., description="Month in YYYY-MM format")):
    cache_key = f"unit-economics:{month}"
    cached = get_cached(cache_key)
    if cached is not None:
        return cached

    cost_rows = run_query(
        """
        select coalesce(sum(cost_usd), 0) as total_cost_usd
        from analytics.unified_cost_model
        where to_char(usage_date, 'YYYY-MM') = %s
        """,
        (month,),
    )
    revenue_rows = run_query(
        """
        select coalesce(sum(amount_usd), 0) as total_revenue_usd
        from analytics.stg_stripe_revenue
        where to_char(transaction_date, 'YYYY-MM') = %s
        """,
        (month,),
    )

    total_cost = float(cost_rows[0]["total_cost_usd"])
    total_revenue = float(revenue_rows[0]["total_revenue_usd"])

    pct = (total_cost / total_revenue * 100) if total_revenue > 0 else None

    result = {
        "month": month,
        "total_cost_usd": total_cost,
        "total_revenue_usd": total_revenue,
        "cost_as_pct_of_revenue": pct,
    }
    set_cached(cache_key, result)
    return result