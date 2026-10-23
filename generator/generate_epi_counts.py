"""S4 part A: true daily onset counts per district x disease, with injected outbreaks.

Real inputs : data_ref/district_ref.csv, data_raw/weather_daily.csv (Open-Meteo)
SYNTHETIC outputs:
  data_raw/synthetic/true_daily_counts.csv            district, disease, date, expected, cases
  data_raw/synthetic/generation_params.json
  data_raw/ground_truth/ground_truth_outbreaks.csv    (detection code must NEVER read this)
Deterministic for a given --seed. --sf scales the volume.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "data_ref" / "district_ref.csv"
WEATHER = ROOT / "data_raw" / "weather_daily.csv"
SYN_DIR = ROOT / "data_raw" / "synthetic"
TRUTH_DIR = ROOT / "data_raw" / "ground_truth"

START, END = "2021-01-01", "2025-12-31"
DISEASES = ["dengue", "diarrhoea", "ili", "malaria"]

# reported cases over the 5 years at SF=1 (national)
TARGET = {"dengue": 1_000_000, "diarrhoea": 8_000_000, "ili": 6_500_000, "malaria": 170_000}
# disease: (seasonal peak day-of-year, seasonal concentration kappa, negative-binomial size k)
SEASON = {"dengue": (255, 1.6, 10), "diarrhoea": (135, 0.9, 20),
          "ili": (205, 1.0, 20), "malaria": (210, 1.5, 6)}
# dengue was worst in 2023 (relative yearly intensity)
DENGUE_YEAR = {2021: 0.8, 2022: 1.0, 2023: 2.3, 2024: 1.1, 2025: 1.0}
# local outbreaks to inject per disease (+1 national surge)
PLAN = {"dengue": 12, "diarrhoea": 8, "ili": 8, "malaria": 4}


def zscore(df):
    sd = df.std(ddof=0).replace(0, 1.0)
    return ((df - df.mean()) / sd).clip(-3, 3)


def load_weather(ref, dates):
    w = pd.read_csv(WEATHER, parse_dates=["date"])

    def piv(col):
        return w.pivot(index="date", columns="district_id", values=col).reindex(
            index=dates, columns=ref.district_id)

    rain, temp = piv("precip_mm"), piv("temp_mean_c")
    missing = int(rain.isna().sum().sum() + temp.isna().sum().sum())
    if missing:
        print(f"warning: {missing} missing weather values were filled")
    return rain.fillna(0.0), temp.ffill().bfill()


def district_weights(ref):
    """Relative share of each disease by district (population x geographic factor)."""
    name = ref.district.str.strip().str.lower()
    pop = ref.population.to_numpy(float)

    def boost(table, default):
        w = np.full(len(ref), default, float)
        for prefix, v in table.items():
            m = name.str.startswith(prefix).to_numpy()
            if not m.any():
                print(f"warning: no district name starts with '{prefix}'")
            w[m] = v
        return w

    return {
        "dengue": pop * boost({"dhaka": 6, "chittagong": 3, "gazipur": 2, "narayanganj": 2,
                               "khulna": 1.5, "barisal": 1.5, "cox": 2, "rajshahi": 1.2,
                               "sylhet": 1.2, "jessore": 1.2, "comilla": 1.2}, 0.5),
        "diarrhoea": pop * 1.0,
        "ili": pop * boost({"dhaka": 1.5, "chittagong": 1.2}, 1.0),
        "malaria": pop * boost({"bandarban": 80, "rangamati": 50, "khagrach": 50,
                                "cox": 8, "sylhet": 2, "maulvibazar": 2}, 0.02),
    }


def seasonal(dates, peak, kappa, n_dist, rng):
    """Annual cycle with district-specific timing and amplitude, mean 1 per district."""
    doy = dates.dayofyear.to_numpy()[None, :]
    shift = rng.uniform(-10, 10, (n_dist, 1))
    kap = kappa * rng.lognormal(0, 0.2, (n_dist, 1))
    s = np.exp(kap * np.cos(2 * np.pi * (doy - peak - shift) / 365.25))
    return s / s.mean(axis=1, keepdims=True)


def bump(dur, peak_frac):
    t = (np.arange(dur) + 0.5) / dur
    return np.where(t < peak_frac, t / peak_frac, (1 - t) / (1 - peak_frac))


def make_outbreaks(rng, ref, dates, mu):
    D, T = len(ref), len(dates)
    mult = {dz: np.ones((D, T)) for dz in DISEASES}
    rows = []

    def add(dz, i, s, dur, peak, peak_frac, kind, oid):
        s, dur = int(s), int(dur)
        e = min(s + dur, T)
        b = bump(dur, peak_frac)[: e - s]
        mult[dz][i, s:e] *= 1 + (peak - 1) * b
        pk = s + min(int(peak_frac * dur), e - s - 1)
        rows.append({"outbreak_id": oid, "disease": dz,
                     "district_id": ref.district_id.iloc[i], "district": ref.district.iloc[i],
                     "start_date": dates[s].date(), "peak_date": dates[pk].date(),
                     "end_date": dates[e - 1].date(), "peak_multiplier": round(peak, 2),
                     "kind": kind})

    n_id = 0
    for dz, n in PLAN.items():
        cands = np.flatnonzero(mu[dz].mean(axis=1) >= 0.5)   # districts with enough cases to see an outbreak
        chosen = rng.choice(cands, size=min(n, len(cands)), replace=False)
        for j, i in enumerate(chosen):
            n_id += 1
            slow = dz == "dengue" and j == 0
            dur = 120 if slow else int(rng.integers(21, 71))
            peak = 3.0 if slow else float(rng.uniform(2.0, 6.0))
            pf = 0.9 if slow else float(rng.uniform(0.35, 0.5))
            s = int(rng.integers(90, T - dur - 1))
            add(dz, i, s, dur, peak, pf, "slow_growth" if slow else "sharp", f"OB{n_id:03d}")

    # nationwide novel-respiratory surge, starting in Dhaka and reaching other districts later
    n_id += 1
    oid = f"OB{n_id:03d}"
    names = ref.district.str.strip().str.lower().to_numpy()
    dhaka = int(np.flatnonzero(np.char.startswith(names.astype(str), "dhaka"))[0])
    lat, lon = ref.lat.to_numpy(), ref.lon.to_numpy()
    dist = np.hypot(lat - lat[dhaka], lon - lon[dhaka])
    delay = np.round(21 * dist / dist.max()).astype(int)
    s0 = dates.get_loc(pd.Timestamp("2022-03-01"))
    for i in range(D):
        add("ili", i, s0 + delay[i], 100, 3.5, 0.5, "national_surge", oid)
    return pd.DataFrame(rows), mult


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sf", type=float, default=0.1, help="scale factor for volume")
    args = ap.parse_args()

    ss = np.random.SeedSequence(args.seed)
    rng_season, rng_out, rng_noise = [np.random.default_rng(s) for s in ss.spawn(3)]

    ref = pd.read_csv(REF).sort_values("district_id").reset_index(drop=True)
    D = len(ref)
    dates = pd.date_range(START, END, freq="D")
    T = len(dates)

    rain, temp = load_weather(ref, dates)
    # weather 4-8 weeks ago (mean over days t-55 .. t-28), standardised per district
    z_rain = zscore(rain.rolling(28).mean().shift(28)).fillna(0.0).to_numpy().T
    z_temp = zscore(temp.rolling(28).mean().shift(28)).fillna(0.0).to_numpy().T
    # extreme rain (3-day total above the district's 95th percentile), felt 3-17 days later
    r3 = rain.rolling(3).sum()
    ext14 = ((r3 > r3.quantile(0.95)).astype(float)
             .rolling(14, min_periods=1).max().shift(3).fillna(0.0).to_numpy().T)
    year_factor = np.array([DENGUE_YEAR[y] for y in dates.year])[None, :]

    climate = {
        "dengue": np.exp(0.30 * z_rain + 0.25 * z_temp) * year_factor,
        "diarrhoea": 1 + 0.8 * ext14,
        "ili": np.ones((D, T)),
        "malaria": np.exp(0.15 * z_rain + 0.20 * z_temp),
    }

    w = district_weights(ref)
    mu = {}
    for dz in DISEASES:
        peak, kappa, _ = SEASON[dz]
        m = (w[dz] / w[dz].sum())[:, None] * seasonal(dates, peak, kappa, D, rng_season) * climate[dz]
        mu[dz] = m * (TARGET[dz] * args.sf / m.sum())      # expected daily cases, national total fixed

    truth, mult = make_outbreaks(rng_out, ref, dates, mu)

    frames = []
    for dz in DISEASES:
        k = SEASON[dz][2]
        mu_out = mu[dz] * mult[dz]
        cases = rng_noise.negative_binomial(k, k / (k + mu_out))   # overdispersed counts
        frames.append(pd.DataFrame({
            "district_id": np.repeat(ref.district_id.to_numpy(), T),
            "disease": dz,
            "date": np.tile(dates, D),
            "expected": np.round(mu_out.ravel(), 3),
            "cases": cases.ravel(),
        }))
    out = pd.concat(frames, ignore_index=True)

    SYN_DIR.mkdir(parents=True, exist_ok=True)
    TRUTH_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(SYN_DIR / "true_daily_counts.csv", index=False)
    truth.to_csv(TRUTH_DIR / "ground_truth_outbreaks.csv", index=False)
    (SYN_DIR / "generation_params.json").write_text(
        json.dumps({"seed": args.seed, "sf": args.sf, "start": START, "end": END}), encoding="utf-8")

    print(f"wrote {len(out):,} rows; outbreaks: {truth.outbreak_id.nunique()} "
          f"({len(truth)} district-level events)")
    print("total cases per disease:", out.groupby("disease").cases.sum().to_dict())
    yearly = out.assign(year=out.date.dt.year).pivot_table(
        index="year", columns="disease", values="cases", aggfunc="sum")
    print(yearly.to_string())
    print(truth.drop_duplicates("outbreak_id").kind.value_counts().to_dict())


if __name__ == "__main__":
    main()