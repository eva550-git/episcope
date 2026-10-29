import pandas as pd

from analytics.optimise.resource_allocator import optimize_resource_allocation, optimize_resource_portfolio


def test_resource_allocation_honours_budget():
    demand = pd.DataFrame(
        [
            {"district_id": "D1", "expected_demand": 200.0},
            {"district_id": "D2", "expected_demand": 280.0},
            {"district_id": "D3", "expected_demand": 320.0},
        ]
    )

    result = optimize_resource_allocation(demand, budget=500.0, equity_floor=0.60)

    assert result["allocated"].sum() <= 500.0 + 1e-6
    assert result["unmet"].sum() >= 0.0
    assert set(result.columns) == {"district_id", "allocated", "unmet", "demand"}


def test_portfolio_allocation_respects_budget_stock_and_equity_floor():
    demand = pd.DataFrame(
        [
            {"district_id": "D1", "resource": "kits", "demand": 80, "current_stock": 40, "population": 100, "unit_cost": 10, "severity_weight": 1},
            {"district_id": "D2", "resource": "kits", "demand": 80, "current_stock": 40, "population": 300, "unit_cost": 10, "severity_weight": 1},
        ]
    )

    result = optimize_resource_portfolio(demand, budget=400, equity_floor=0.25)

    assert result["allocated"].sum() <= 40 + 1e-6
    assert (result["allocated"] * result["unit_cost"]).sum() <= 400 + 1e-6
    total_allocated = result["allocated"].sum()
    population_share = {"D1": 0.25, "D2": 0.75}
    for row in result.itertuples():
        assert row.allocated >= 0.25 * population_share[row.district_id] * total_allocated - 1e-6
