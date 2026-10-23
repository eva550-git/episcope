"""S4 part B: expand true daily counts into encounter-level records (SYNTHETIC DATA).

Inputs : data_raw/synthetic/true_daily_counts.csv   (from generate_epi_counts.py)
         data_raw/synthetic/facility_registry.csv   (from generate_facilities.py)
         data_ref/district_ref.csv
Outputs: data_raw/landing/encounters/encounters_YYYY-MM.csv   one file per REPORT month
         data_raw/ground_truth/ground_truth_artefacts.csv
         data_raw/ground_truth/ground_truth_patient_duplicates.csv
Deterministic for a given --seed. Month files are generated independently, so any month
range gives the same rows as the full run. The detection code must never read ground_truth/.
"""
import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "data_ref" / "district_ref.csv"
COUNTS = ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
REGISTRY = ROOT / "data_raw" / "synthetic" / "facility_registry.csv"
TRUTH_DIR = ROOT / "data_raw" / "ground_truth"

COLS = ["encounter_id", "patient_id", "facility_id", "onset_date", "report_date",
        "age_years", "sex", "icd10_code", "lab_confirmed", "admitted", "outcome"]

BG_TOTAL = 10_000_000          # background (non-target) encounters over 5 years at SF=1
BG_K = 30                      # negative-binomial size for background counts

# disease -> (codes, probabilities)
CODES = {
    "dengue": (["A90", "A91"], [0.80, 0.20]),
    "diarrhoea": (["A09", "A00"], [0.90, 0.10]),
    "ili": (["J11", "J10"], [0.60, 0.40]),
    "malaria": (["B54", "B50", "B51", "B52"], [0.15, 0.30, 0.50, 0.05]),
    "other": (["I10", "E11", "J18", "K29", "N39", "R50", "A01", "J45", "K52", "L03", "M54", "S06"],
              [0.12, 0.10, 0.10, 0.08, 0.10, 0.12, 0.04, 0.08, 0.06, 0.07, 0.08, 0.05]),
}
# code -> (P lab confirmed, P admitted at baseline, P death if admitted)
RATES = {
    "A90": (0.40, 0.10, 0.005), "A91": (0.60, 0.60, 0.050),
    "A09": (0.08, 0.05, 0.003), "A00": (0.50, 0.35, 0.030),
    "J11": (0.08, 0.04, 0.010), "J10": (0.35, 0.06, 0.015),
    "B54": (0.60, 0.12, 0.010), "B50": (0.80, 0.30, 0.030),
    "B51": (0.75, 0.10, 0.005), "B52": (0.70, 0.08, 0.005),
}
OTHER_RATE = (0.05, 0.07, 0.010)
# age bands 0-4, 5-14, 15-29, 30-44, 45-59, 60-89
BANDS = np.array([0, 5, 15, 30, 45, 60, 90])
AGE_P = {"dengue": [.08, .25, .35, .20, .09, .03], "diarrhoea": [.35, .15, .20, .14, .09, .07],
         "ili": [.20, .20, .25, .17, .11, .07], "malaria": [.05, .15, .30, .28, .15, .07],
         "other": [.08, .10, .22, .22, .20, .18]}
FEMALE_P = {"dengue": .46, "diarrhoea": .50, "ili": .50, "malaria": .40, "other": .55}
# fixed national holidays (month, day): treated like a weekend for report submission
HOLIDAYS = {(2, 21), (3, 26), (4, 14), (5, 1), (12, 16), (12, 25)}

PATIENT_REPEAT_RATE = 0.010    # same patient seen at a second facility within a week
RESUBMIT_RATE = 0.020          # exact resubmitted records (duplicate encounter_id)
ORPHAN_RATE = 0.005            # facility_id not in the registry
BAD_AGE_RATE = 0.007
BAD_SEX_RATE = 0.003
DEFER_RATE = 0.70              # share of weekend/holiday submissions pushed to the next working day


def is_off_day(d):
    wd = d.dt.weekday
    hol = pd.Series(list(zip(d.dt.month, d.dt.day)), index=d.index).isin(HOLIDAYS)
    return ((wd == 4) | (wd == 5)) | hol            # Friday, Saturday (Bangladesh weekend)


def report_dates(onset, rng):
    """Right-skewed delay (0-21 days), fewer reports on weekends/holidays."""
    n = len(onset)
    delay = np.floor(rng.lognormal(0.9, 0.75, n))
    tail = rng.random(n) < 0.04                      # 4% very late reports
    delay = np.where(tail, rng.integers(8, 22, n), delay)
    delay = np.minimum(delay, 21).astype(int)
    first = onset + pd.to_timedelta(delay, unit="D")
    move = pd.Series(rng.random(n) < DEFER_RATE, index=onset.index)
    # deferred weekend/holiday submissions are spread over the next 1-3 days, then skip off days
    push = pd.to_timedelta(rng.integers(1, 4, n), unit="D")
    rep = first.where(~(is_off_day(first) & move), first + push)
    for _ in range(4):
        off = is_off_day(rep) & move
        if not off.any():
            break
        rep = rep.where(~off, rep + pd.Timedelta(days=1))
    too_late = (rep - onset).dt.days > 21
    return rep.where(~too_late, first)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sf", type=float, default=0.1, help="must equal the --sf used for the counts")
    ap.add_argument("--from-date", default=None, help="first onset date (default: start of counts)")
    ap.add_argument("--to-date", default=None, help="last onset date (default: end of counts)")
    ap.add_argument("--out-dir", default=str(ROOT / "data_raw" / "landing" / "encounters"))
    ap.add_argument("--silent-start", default="2024-06-03", help="reporting artefact: facility goes silent")
    ap.add_argument("--silent-days", type=int, default=21)
    args = ap.parse_args()
    t0 = time.time()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("encounters_*.csv"):
        old.unlink()
    TRUTH_DIR.mkdir(parents=True, exist_ok=True)

    ref = pd.read_csv(REF).sort_values("district_id").reset_index(drop=True)
    D = len(ref)
    d_index = {d: i for i, d in enumerate(ref.district_id)}
    reg = pd.read_csv(REGISTRY).sort_values("facility_id").reset_index(drop=True)
    reg["didx"] = reg.district_id.map(d_index)
    assert reg.didx.is_monotonic_increasing, "facilities must be grouped by district"
    fac_ids = reg.facility_id.to_numpy()
    starts = np.searchsorted(reg.didx.to_numpy(), np.arange(D))
    ends = np.searchsorted(reg.didx.to_numpy(), np.arange(D), side="right")
    assert (ends - starts >= 1).all(), "every district needs a facility"
    cum = []
    for d in range(D):
        w = reg.beds.to_numpy(float)[starts[d]:ends[d]]
        c = np.cumsum(w / w.sum())
        c[-1] = 1.0 + 1e-9
        cum.append(c)

    def sample_facility(didx, rng):
        u = rng.random(len(didx))
        out = np.empty(len(didx), dtype=np.int64)
        order = np.argsort(didx, kind="stable")
        bounds = np.searchsorted(didx[order], np.arange(D + 1))
        for d in range(D):
            sl = order[bounds[d]:bounds[d + 1]]
            if len(sl):
                out[sl] = starts[d] + np.minimum(np.searchsorted(cum[d], u[sl], side="right"),
                                                 ends[d] - starts[d] - 1)
        return out

    # artefact facility: biggest facility in the most populous district
    big = int(ref.population.to_numpy().argmax())
    cand = reg[reg.didx == big]
    art_fac = cand.loc[cand.beds.idxmax(), "facility_id"]
    a_start = pd.Timestamp(args.silent_start)
    a_end = a_start + pd.Timedelta(days=args.silent_days - 1)
    backfill = a_end + pd.Timedelta(days=1)
    art_count = 0

    counts = pd.read_csv(COUNTS, parse_dates=["date"])
    counts["didx"] = counts.district_id.map(d_index)
    med = counts.groupby(["district_id", "disease"]).expected.transform("median").clip(lower=1e-9)
    counts["sev"] = np.log2((counts.expected / med).clip(lower=1e-9)).clip(0, 3)
    all_days = counts.date.nunique()
    lo = pd.Timestamp(args.from_date) if args.from_date else counts.date.min()
    hi = pd.Timestamp(args.to_date) if args.to_date else counts.date.max()
    counts = counts[(counts.date >= lo) & (counts.date <= hi)]
    pop_share = ref.population.to_numpy(float) / ref.population.sum()

    months = pd.period_range(lo, hi, freq="M")
    total = dup_total = orphan_total = repeat_total = 0
    pair_rows = []
    written = {}

    for m_idx, per in enumerate(months):
        rng = np.random.default_rng([args.seed, 17, per.year * 12 + per.month])
        mdays = pd.date_range(max(per.start_time, lo), min(per.end_time.normalize(), hi), freq="D")
        sub = counts[(counts.date >= mdays[0]) & (counts.date <= mdays[-1]) & (counts.cases > 0)]

        # --- expand counts to one row per case ------------------------------------------------
        rep_idx = np.repeat(np.arange(len(sub)), sub.cases.to_numpy())
        base = pd.DataFrame({
            "didx": sub.didx.to_numpy()[rep_idx],
            "group": sub.disease.to_numpy()[rep_idx],
            "onset": sub.date.to_numpy()[rep_idx],
            "sev": sub.sev.to_numpy()[rep_idx],
        })
        mu = pop_share[:, None] * BG_TOTAL * args.sf / all_days * np.ones((D, len(mdays)))
        bg = rng.negative_binomial(BG_K, BG_K / (BG_K + mu))
        bg_d, bg_t = np.nonzero(bg)
        bg_rep = np.repeat(np.arange(len(bg_d)), bg[bg_d, bg_t])
        other = pd.DataFrame({
            "didx": bg_d[bg_rep], "group": "other",
            "onset": mdays.to_numpy()[bg_t[bg_rep]], "sev": 0.0,
        })
        df = pd.concat([base, other], ignore_index=True)
        n = len(df)
        assert n < 10**7, "too many rows in one month for the id scheme"
        df["onset"] = pd.to_datetime(df["onset"])

        # --- clinical attributes -------------------------------------------------------------
        df["icd10_code"] = ""
        df["age_years"] = 0
        df["sex"] = ""
        for g, (codes, probs) in CODES.items():
            mask = (df.group == g).to_numpy()
            k = int(mask.sum())
            if not k:
                continue
            df.loc[mask, "icd10_code"] = rng.choice(codes, size=k, p=probs)
            band = rng.choice(6, size=k, p=AGE_P[g])
            df.loc[mask, "age_years"] = BANDS[band] + np.floor(rng.random(k) * (BANDS[band + 1] - BANDS[band])).astype(int)
            df.loc[mask, "sex"] = np.where(rng.random(k) < FEMALE_P[g], "F", "M")
        df["fac_idx"] = sample_facility(df.didx.to_numpy(), rng)

        # --- ids ---------------------------------------------------------------------------
        df["eid_num"] = np.arange(n)
        df["pid_num"] = (per.year * 12 + per.month) * 10**7 + np.arange(n)

        # --- same patient seen again at another facility within a week ----------------------
        cand = np.flatnonzero((df.group != "other").to_numpy()
                              & ((ends - starts)[df.didx.to_numpy()] > 1))
        pick = cand[rng.random(len(cand)) < PATIENT_REPEAT_RATE / 0.62]   # ~62% of rows are target diseases
        rep = df.iloc[pick].copy()
        if len(rep):
            span = (ends - starts)[rep.didx.to_numpy()]
            old = rep.fac_idx.to_numpy()
            s0 = starts[rep.didx.to_numpy()]
            rep["fac_idx"] = s0 + (old - s0 + rng.integers(1, 10**6, len(rep)) % (span - 1) + 1) % span
            rep["onset"] = rep.onset + pd.to_timedelta(rng.integers(0, 6, len(rep)), unit="D")
            rep["eid_num"] = n + np.arange(len(rep))
            rep["pid_num"] = df.pid_num.to_numpy()[pick]          # same patient
            pair_rows.append(pd.DataFrame({
                "original_encounter_id": [f"E{per.year}{per.month:02d}{i:07d}" for i in df.eid_num.to_numpy()[pick]],
                "repeat_encounter_id": [f"E{per.year}{per.month:02d}{i:07d}" for i in rep.eid_num.to_numpy()],
            }))
            df = pd.concat([df, rep], ignore_index=True)
        repeat_total += len(rep)
        n_all = len(df)

        # --- outcomes (severity cascade rises during outbreaks) --------------------------------
        lab_p = np.array([RATES.get(c, OTHER_RATE)[0] for c in df.icd10_code])
        adm_p = np.array([RATES.get(c, OTHER_RATE)[1] for c in df.icd10_code])
        die_p = np.array([RATES.get(c, OTHER_RATE)[2] for c in df.icd10_code])
        adm_p = np.minimum(0.95, adm_p * (1 + 0.25 * df.sev.to_numpy()))
        df["lab_confirmed"] = (rng.random(n_all) < lab_p).astype(int)
        df["admitted"] = (rng.random(n_all) < adm_p).astype(int)
        u = rng.random(n_all)
        adm = df.admitted.to_numpy() == 1
        died = np.where(adm, u < die_p, u < 0.0003)
        referred = np.where(adm, (u >= die_p) & (u < die_p + 0.05), (u >= 0.0003) & (u < 0.0603))
        df["outcome"] = np.where(died, "died", np.where(referred, "referred", "recovered"))

        # --- report dates, weekends, reporting artefact ---------------------------------------
        df["onset_date"] = df.onset
        df["report_date"] = report_dates(df.onset_date, rng)
        df["facility_id"] = fac_ids[df.fac_idx.to_numpy()]
        silent = ((df.facility_id == art_fac) & (df.report_date >= a_start) & (df.report_date <= a_end))
        art_count += int(silent.sum())
        df.loc[silent, "report_date"] = backfill

        # --- data defects -----------------------------------------------------------------------
        orphan = rng.random(n_all) < ORPHAN_RATE
        df.loc[orphan, "facility_id"] = ["F9%03d" % v for v in rng.integers(0, 1000, int(orphan.sum()))]
        orphan_total += int(orphan.sum())
        bad_age = rng.random(n_all) < BAD_AGE_RATE
        df.loc[bad_age, "age_years"] = rng.choice([-5, 150, 212, 999], size=int(bad_age.sum()))
        bad_sex = rng.random(n_all) < BAD_SEX_RATE
        df.loc[bad_sex, "sex"] = "X"

        eid_prefix = f"E{per.year}{per.month:02d}"
        df["encounter_id"] = [f"{eid_prefix}{i:07d}" for i in df.eid_num.to_numpy()]
        pid = (df.pid_num.to_numpy().astype(np.int64) * 2654435761) % 10**13   # bijection: unique ids
        df["patient_id"] = [f"{v:013d}" for v in pid]

        # --- resubmitted duplicates -------------------------------------------------------------
        dup_pick = rng.random(n_all) < RESUBMIT_RATE
        dups = df[dup_pick].copy()
        resent = dups.report_date + pd.to_timedelta(rng.integers(0, 8, len(dups)), unit="D")
        dups["report_date"] = resent.where(resent <= dups.onset_date + pd.Timedelta(days=21),
                                           dups.report_date)   # keep within the 0-21 day window
        dup_total += len(dups)
        df = pd.concat([df, dups], ignore_index=True)

        # --- write one file per report month (append) ------------------------------------------
        df["onset_date"] = df.onset_date.dt.strftime("%Y-%m-%d")
        df["report_month"] = df.report_date.dt.strftime("%Y-%m")
        df["report_date"] = df.report_date.dt.strftime("%Y-%m-%d")
        for rm, part in df.groupby("report_month"):
            path = out_dir / f"encounters_{rm}.csv"
            part[COLS].to_csv(path, mode="a", header=not path.exists(), index=False)
            written[rm] = True
        total += len(df)
        print(f"{per}: {len(df):>9,} rows", flush=True)

    pd.DataFrame([{
        "artefact_id": "AR001", "type": "silent_then_backfill", "facility_id": art_fac,
        "silent_start": a_start.date(), "silent_end": a_end.date(), "backfill_date": backfill.date(),
        "records_delayed": art_count}]).to_csv(TRUTH_DIR / "ground_truth_artefacts.csv", index=False)
    if pair_rows:
        pd.concat(pair_rows).to_csv(TRUTH_DIR / "ground_truth_patient_duplicates.csv", index=False)

    print(f"\ntotal rows written : {total:,}")
    print(f"  resubmitted duplicates : {dup_total:,} ({dup_total / total:.2%})")
    print(f"  cross-facility repeats : {repeat_total:,}")
    print(f"  orphan facility keys   : {orphan_total:,} ({orphan_total / total:.2%})")
    print(f"  artefact facility {art_fac}: {art_count:,} reports held until {backfill.date()}")
    print(f"files: {len(written)} in {out_dir}   time: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()