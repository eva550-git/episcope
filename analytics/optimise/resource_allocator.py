"""Minimal resource allocation optimization using PuLP."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pulp

ROOT = Path(__file__).resolve().parents[2]


def optimize_resource_allocation(demand: pd.DataFrame, budget: float = 1_000_000.0, equity_floor: float = 0.60):
    """Minimize unmet demand subject to a national budget and district equity floor."""
    empty = pd.DataFrame(columns=["district_id", "allocated", "unmet", "demand"])
    if demand is None or demand.empty:
        return empty

    demand = demand.copy()
    if "district_id" not in demand.columns:
        raise ValueError("Demand data must include a district_id column.")

    demand["district_id"] = demand["district_id"].astype(str)
    value_col = "expected_demand" if "expected_demand" in demand.columns else "cases"
    demand[value_col] = pd.to_numeric(demand.get(value_col, 0.0), errors="coerce").fillna(0.0)
    demand["expected_demand"] = demand[value_col].clip(lower=0.0)
    demand = demand.groupby("district_id", as_index=False)["expected_demand"].sum()
    demand = demand.sort_values("district_id").reset_index(drop=True)

    if demand.empty:
        return empty

    districts = demand["district_id"].tolist()
    required = {d: float(row["expected_demand"]) for _, row in demand.iterrows() for d in [row["district_id"]]}
    if not districts:
        return empty

    total_required = sum(required.values())
    if budget <= 0:
        return pd.DataFrame([
            {
                "district_id": d,
                "allocated": 0.0,
                "unmet": round(required[d], 2),
                "demand": round(required[d], 2),
            }
            for d in districts
        ])

    floor_limit = min(1.0, max(0.0, equity_floor))
    if total_required > 0 and floor_limit > 0 and sum(required[d] * floor_limit for d in districts) > budget:
        # Keep the floor feasible by reducing the floor to the largest value that still fits.
        floor_limit = min(floor_limit, budget / total_required)

    decision = {d: pulp.LpVariable(f"x_{d}", lowBound=0, upBound=required[d]) for d in districts}
    unmet = {d: pulp.LpVariable(f"u_{d}", lowBound=0) for d in districts}

    problem = pulp.LpProblem("district_resource_allocation", pulp.LpMinimize)
    problem += pulp.lpSum(unmet[d] for d in districts)

    for d in districts:
        problem += decision[d] <= required[d]
        problem += decision[d] + unmet[d] >= required[d]
        problem += decision[d] >= required[d] * floor_limit

    problem += pulp.lpSum(decision[d] for d in districts) <= budget
    problem.solve(pulp.PULP_CBC_CMD(msg=False))

    rows = []
    for d in districts:
        req = required[d]
        alloc = float(pulp.value(decision[d]) or 0.0)
        shortfall = max(req - alloc, 0.0)
        rows.append({
            "district_id": d,
            "allocated": round(alloc, 2),
            "unmet": round(shortfall, 2),
            "demand": round(req, 2),
        })

    return pd.DataFrame(rows)


def optimize_resource_portfolio(
    demand: pd.DataFrame,
    budget: float,
    equity_floor: float = 0.0,
) -> pd.DataFrame:
    """Allocate several constrained resource pools across districts under one budget."""
    columns = [
        "district_id", "resource", "demand", "current_stock",
        "allocated", "unmet", "unit_cost",
    ]
    if demand is None or demand.empty:
        return pd.DataFrame(columns=columns)

    required = {
        "district_id", "resource", "demand", "current_stock",
        "population", "unit_cost", "severity_weight",
    }
    missing = required - set(demand.columns)
    if missing:
        raise ValueError(f"Resource demand is missing columns: {', '.join(sorted(missing))}.")
    if budget < 0:
        raise ValueError("Budget cannot be negative.")

    data = demand.copy()
    data["district_id"] = data["district_id"].astype(str)
    for column in ("demand", "current_stock", "population", "unit_cost", "severity_weight"):
        data[column] = pd.to_numeric(data[column], errors="raise")
        if (data[column] < 0).any():
            raise ValueError(f"{column} cannot be negative.")
    if (data["unit_cost"] <= 0).any():
        raise ValueError("Resource unit costs must be greater than zero.")
    if data.duplicated(["district_id", "resource"]).any():
        raise ValueError("Resource demand must have one row per district and resource.")

    districts = sorted(data["district_id"].unique())
    population = data.groupby("district_id")["population"].max().to_dict()
    population_total = sum(population.values())
    population_share = {
        district: (population[district] / population_total if population_total else 1 / len(districts))
        for district in districts
    }
    resources = sorted(data["resource"].unique())
    rows = {
        (row.district_id, row.resource): row
        for row in data.itertuples(index=False)
    }
    allocation = {
        key: pulp.LpVariable(
            f"x_{key[0]}_{key[1]}",
            lowBound=0,
            upBound=float(row.demand),
        )
        for key, row in rows.items()
    }
    unmet = {
        key: pulp.LpVariable(f"u_{key[0]}_{key[1]}", lowBound=0)
        for key in rows
    }
    problem = pulp.LpProblem("district_resource_portfolio", pulp.LpMinimize)
    problem += pulp.lpSum(
        max(float(row.severity_weight), 0.0)
        * unmet[key] / max(float(row.demand), 1.0)
        for key, row in rows.items()
    )

    for key, row in rows.items():
        problem += allocation[key] + unmet[key] >= float(row.demand)
    for resource in resources:
        resource_keys = [(district, resource) for district in districts if (district, resource) in rows]
        available = sum(float(rows[key].current_stock) for key in resource_keys)
        problem += pulp.lpSum(allocation[key] for key in resource_keys) <= available
        floor = min(max(float(equity_floor), 0.0), 1.0)
        if floor:
            total_allocation = pulp.lpSum(allocation[key] for key in resource_keys)
            for key in resource_keys:
                problem += allocation[key] >= floor * population_share[key[0]] * total_allocation

    problem += pulp.lpSum(
        allocation[key] * float(row.unit_cost)
        for key, row in rows.items()
    ) <= budget
    status = problem.solve(pulp.PULP_CBC_CMD(msg=False))
    if pulp.LpStatus[status] != "Optimal":
        raise RuntimeError(f"Resource allocation did not reach an optimal solution: {pulp.LpStatus[status]}.")

    result = []
    for key, row in rows.items():
        allocated = float(pulp.value(allocation[key]) or 0.0)
        result.append(
            {
                "district_id": key[0],
                "resource": key[1],
                "demand": float(row.demand),
                "current_stock": float(row.current_stock),
                "allocated": allocated,
                "unmet": max(float(row.demand) - allocated, 0.0),
                "unit_cost": float(row.unit_cost),
            }
        )
    return pd.DataFrame(result, columns=columns)


def build_sample_demand():
    ref = pd.read_csv(ROOT / "data_ref" / "district_ref.csv")
    top = ref.head(10).copy()
    top["expected_demand"] = top["population"] / 1000.0
    return top[["district_id", "expected_demand"]]


if __name__ == "__main__":
    result = optimize_resource_allocation(build_sample_demand())
    print(result.to_string(index=False))
