"""Build data_ref/district_ref.csv: 64 districts + centroids + population.

Inputs : data_raw/districts.geojson, data_raw/population_by_district.csv
Output : data_ref/district_ref.csv
Stops with a clear message (and writes nothing) if district names do not match.
"""
import csv
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GEOJSON = ROOT / "data_raw" / "districts.geojson"
POP = ROOT / "data_raw" / "population_by_district.csv"
OUT = ROOT / "data_ref" / "district_ref.csv"

# Fill in only if the name check reports mismatches:
# {"name in population file (lowercase)": "name in geojson (lowercase)"}
ALIASES = {}


def norm(s):
    s = str(s).strip().lower()
    return ALIASES.get(s, s)


def ring_centroid(ring):
    """Area-weighted centroid of one ring (shoelace). Returns (signed_area, cx, cy)."""
    a = cx = cy = 0.0
    for (x0, y0), (x1, y1) in zip(ring, ring[1:]):
        cross = x0 * y1 - x1 * y0
        a += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    a *= 0.5
    if a == 0:
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        return 0.0, sum(xs) / len(xs), sum(ys) / len(ys)
    return a, cx / (6 * a), cy / (6 * a)


def geometry_centroid(geom):
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    tot = sx = sy = 0.0
    for poly in polys:
        area, cx, cy = ring_centroid([tuple(p[:2]) for p in poly[0]])  # outer ring
        area = abs(area)
        tot += area
        sx += cx * area
        sy += cy * area
    return sx / tot, sy / tot  # lon, lat


def main():
    with open(GEOJSON, encoding="utf-8") as f:
        gj = json.load(f)
    rows = []
    for feat in gj["features"]:
        p = feat["properties"]
        lon, lat = geometry_centroid(feat["geometry"])
        rows.append({
            "district_id": p["adm2_pcode"],
            "district": p["adm2_en"].strip(),
            "division_id": p["adm1_pcode"],
            "division": p["adm1_en"].strip(),
            "lat": round(lat, 4),
            "lon": round(lon, 4),
        })
    ref = pd.DataFrame(rows).sort_values("district_id")

    pop = pd.read_csv(POP)
    pop["key"] = pop["district"].map(norm)
    ref["key"] = ref["district"].map(norm)

    only_ref = sorted(set(ref.key) - set(pop.key))
    only_pop = sorted(set(pop.key) - set(ref.key))
    if only_ref or only_pop:
        print("NAME MISMATCH - nothing written")
        print("in geojson only:", only_ref)
        print("in population file only:", only_pop)
        return

    pop["population"] = pop["population_estimate_2023"].round().astype("int64")
    out = ref.merge(pop[["key", "population"]], on="key", how="left").drop(columns="key")

    OUT.parent.mkdir(exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"OK: {len(out)} districts, {out.division.nunique()} divisions, "
          f"total population = {out.population.sum():,} -> {OUT}")
    print(out.sort_values("population", ascending=False).head(3)[["district", "population"]].to_string(index=False))


if __name__ == "__main__":
    main()