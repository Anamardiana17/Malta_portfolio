from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd

BASE = Path.cwd()
DP = BASE / "data_processed" / "operating_model"
DP.mkdir(parents=True, exist_ok=True)

THERAPIST_FP = DP / "therapist_operating_profile_master.csv"
ROSTER_FP = DP / "outlet_roster_quality_design_output.csv"

OUTLET_SUMMARY_SRC = DP / "outlet_management_summary.csv"
THERAPIST_COACHING_SRC = DP / "therapist_coaching_summary.csv"
MANAGER_QUEUE_SRC = DP / "manager_action_queue.csv"

OUTLET_SUMMARY_OUT = DP / "outlet_management_summary_modeled.csv"
THERAPIST_COACHING_OUT = DP / "therapist_coaching_summary_modeled.csv"
MANAGER_QUEUE_OUT = DP / "manager_action_queue_modeled.csv"


def read_csv_safe(fp: Path) -> pd.DataFrame:
    if fp.exists():
        return pd.read_csv(fp)
    return pd.DataFrame()


def first_existing(df: pd.DataFrame, candidates: list[str], default=None):
    for c in candidates:
        if c in df.columns:
            return c
    return default


def choose_outlet_column(df: pd.DataFrame) -> str | None:
    return first_existing(
        df,
        [
            "outlet_name",
            "outlet",
            "outlet_id",
            "home_outlet_name",
            "spa_name",
            "site_name",
            "location_name",
            "branch_name",
        ],
    )


def choose_therapist_id_column(df: pd.DataFrame) -> str | None:
    return first_existing(
        df,
        [
            "therapist_id",
            "modeled_therapist_id",
            "employee_id",
            "staff_id",
            "resource_id",
            "person_id",
        ],
    )


def choose_therapist_name_column(df: pd.DataFrame) -> str | None:
    return first_existing(
        df,
        [
            "therapist_name",
            "modeled_therapist_name",
            "employee_name",
            "staff_name",
            "resource_name",
            "person_name",
        ],
    )


def pick_series(df: pd.DataFrame, candidates: list[str], default_value=""):
    col = first_existing(df, candidates)
    if col is not None:
        return df[col]
    return pd.Series([default_value] * len(df), index=df.index)


def build_outlet_core(therapists: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    outlet_col = choose_outlet_column(therapists)
    therapist_id_col = choose_therapist_id_column(therapists)
    therapist_name_col = choose_therapist_name_column(therapists)

    if outlet_col is None or therapist_id_col is None:
        raise ValueError(
            f"therapist_operating_profile_master.csv must contain outlet and therapist columns. "
            f"Detected columns: {list(therapists.columns)}"
        )

    t = therapists.copy()
    t[outlet_col] = t[outlet_col].astype(str).str.strip()
    t[therapist_id_col] = t[therapist_id_col].astype(str).str.strip()

    if therapist_name_col is None:
        therapist_name_col = "therapist_name"
        t[therapist_name_col] = t[therapist_id_col]

    score_col = first_existing(
        t,
        [
            "service_consistency_score_0_100",
            "therapist_service_consistency_score_0_100",
            "consistency_score_0_100",
        ],
    )
    archetype_col = first_existing(
        t,
        [
            "therapist_archetype",
            "deployment_archetype",
            "service_archetype",
            "role_archetype",
        ],
    )

    outlet_core = (
        t.groupby(outlet_col, dropna=False)
        .agg(
            modeled_therapist_pool_count=(therapist_id_col, "nunique"),
            sample_modeled_therapist_ids=(
                therapist_id_col,
                lambda s: " | ".join(sorted(pd.Series(s).dropna().astype(str).unique())[:5]),
            ),
            sample_modeled_therapist_names=(
                therapist_name_col,
                lambda s: " | ".join(sorted(pd.Series(s).dropna().astype(str).unique())[:5]),
            ),
        )
        .reset_index()
        .rename(columns={outlet_col: "outlet_name"})
    )

    if archetype_col is not None:
        arche = (
            t.groupby([outlet_col, archetype_col], dropna=False)[therapist_id_col]
            .nunique()
            .reset_index(name="therapist_count")
            .sort_values([outlet_col, "therapist_count", archetype_col], ascending=[True, False, True])
        )
        arch_map = (
            arche.groupby(outlet_col)
            .head(2)
            .groupby(outlet_col)
            .apply(lambda x: " | ".join(f"{r[archetype_col]} ({int(r['therapist_count'])})" for _, r in x.iterrows()))
            .reset_index(name="modeled_therapist_archetype_mix")
            .rename(columns={outlet_col: "outlet_name"})
        )
        outlet_core = outlet_core.merge(arch_map, on="outlet_name", how="left")
    else:
        outlet_core["modeled_therapist_archetype_mix"] = "Balanced outlet deployment mix"

    if score_col is not None:
        score_map = (
            t.groupby(outlet_col)[score_col]
            .mean()
            .round(2)
            .reset_index(name="avg_modeled_service_consistency_score_0_100")
            .rename(columns={outlet_col: "outlet_name"})
        )
        outlet_core = outlet_core.merge(score_map, on="outlet_name", how="left")
    else:
        outlet_core["avg_modeled_service_consistency_score_0_100"] = pd.NA

    if not roster.empty:
        r = roster.copy()
        r_outlet = choose_outlet_column(r)
        if r_outlet:
            keep = [r_outlet]
            rename_map = {r_outlet: "outlet_name"}

            mappings = [
                (first_existing(r, ["safe_roster_headcount", "revised_safe_roster_headcount", "recommended_safe_roster_headcount", "minimum_safe_roster_headcount", "required_therapist_headcount", "recommended_therapist_headcount"]), "revised_safe_roster_headcount"),
                (first_existing(r, ["minimum_opening_team", "min_opening_team", "opening_team_min"]), "minimum_opening_team"),
                (first_existing(r, ["service_consistency_buffer", "service_consistency_buffer_headcount", "buffer_headcount"]), "service_consistency_buffer_headcount"),
                (first_existing(r, ["quality_adjusted_productive_therapist_day", "quality_adjusted_productive_day"]), "quality_adjusted_productive_therapist_day"),
            ]

            for src, dst in mappings:
                if src is not None:
                    keep.append(src)
                    rename_map[src] = dst

            roster_keep = r[keep].drop_duplicates().rename(columns=rename_map)
            outlet_core = outlet_core.merge(roster_keep, on="outlet_name", how="left")

    if "revised_safe_roster_headcount" in outlet_core.columns:
        def alignment(row):
            pool = row["modeled_therapist_pool_count"]
            target = row["revised_safe_roster_headcount"]
            if pd.isna(target):
                return "Modeled therapist pool aligned to service-consistency deployment logic"
            gap = pool - target
            if gap > 0:
                return "Modeled therapist pool sits above minimum safe roster requirement"
            if gap == 0:
                return "Modeled therapist pool fully aligned to revised safe roster requirement"
            return "Modeled therapist pool below safe roster requirement - review source therapist master"

        outlet_core["modeled_roster_alignment_read"] = outlet_core.apply(alignment, axis=1)
    else:
        outlet_core["modeled_roster_alignment_read"] = "Modeled therapist pool aligned to service-consistency deployment logic"

    return outlet_core


def build_outlet_management_summary_modeled(outlet_core: pd.DataFrame, src: pd.DataFrame) -> pd.DataFrame:
    if src.empty:
        out = outlet_core.copy()
    else:
        s = src.copy()
        outlet_col = choose_outlet_column(s)
        if outlet_col and outlet_col != "outlet_name":
            s = s.rename(columns={outlet_col: "outlet_name"})
        elif outlet_col is None:
            s["outlet_name"] = "UNKNOWN"
        out = s.merge(outlet_core, on="outlet_name", how="outer")

    out["modeled_therapist_structure_note"] = "Therapist pool reflects outlet-coded individual therapists and revised roster-quality assumptions"
    out["single_therapist_outlet_flag"] = out["modeled_therapist_pool_count"].fillna(0).astype(float).le(1)
    out["portfolio_positioning_note"] = "Commercial and operational management view anchored to outlet scale, service consistency, and safe therapist deployment"
    return out.sort_values("outlet_name").reset_index(drop=True)


def build_therapist_coaching_summary_modeled(therapists: pd.DataFrame, src: pd.DataFrame) -> pd.DataFrame:
    t = therapists.copy()

    outlet_col = choose_outlet_column(t)
    therapist_id_col = choose_therapist_id_column(t)
    therapist_name_col = choose_therapist_name_column(t)

    if outlet_col is None or therapist_id_col is None:
        raise ValueError("Therapist coaching build failed because outlet or therapist ID column was not found.")

    if therapist_name_col is None:
        therapist_name_col = "therapist_name"
        t[therapist_name_col] = t[therapist_id_col]

    modeled = pd.DataFrame({
        "outlet_name": t[outlet_col].astype(str).str.strip(),
        "therapist_id": t[therapist_id_col].astype(str).str.strip(),
        "therapist_name": t[therapist_name_col].astype(str).str.strip(),
        "therapist_archetype": pick_series(t, ["therapist_archetype", "deployment_archetype", "service_archetype", "role_archetype"], "balanced"),
        "coaching_priority": pick_series(t, ["coaching_priority", "coaching_focus", "development_priority"], "Protect service consistency and strengthen outlet-fit deployment"),
        "deployment_recommendation": pick_series(t, ["deployment_recommendation", "service_mix_recommendation", "allocation_recommendation"], "Deploy against outlet demand pattern and service-consistency guardrails"),
        "service_consistency_score_0_100": pick_series(t, ["service_consistency_score_0_100", "therapist_service_consistency_score_0_100", "consistency_score_0_100"], pd.NA),
        "coaching_need_score_0_100": pick_series(t, ["coaching_need_score_0_100", "development_need_score_0_100", "performance_gap_score_0_100"], pd.NA),
    })

    modeled["therapist_name"] = modeled["therapist_id"].astype(str).str.replace("_TH_", " Therapist ", regex=False)
    modeled["coaching_positioning_note"] = "Modeled therapist coaching view reflects individual therapist deployment rather than generic placeholder staffing"

    if src.empty:
        out = modeled
    else:
        s = src.copy()
        s_outlet = choose_outlet_column(s)
        s_tid = choose_therapist_id_column(s)

        if s_outlet and s_outlet != "outlet_name":
            s = s.rename(columns={s_outlet: "outlet_name"})
        if s_tid and s_tid != "therapist_id":
            s = s.rename(columns={s_tid: "therapist_id"})

        out = s.merge(modeled, on=["outlet_name", "therapist_id"], how="outer", suffixes=("_old", ""))

    return out.sort_values(["outlet_name", "therapist_id"]).reset_index(drop=True)


def build_manager_action_queue_modeled(outlet_core: pd.DataFrame, therapists: pd.DataFrame, src: pd.DataFrame) -> pd.DataFrame:
    t = therapists.copy()
    outlet_col = choose_outlet_column(t)
    therapist_id_col = choose_therapist_id_column(t)
    therapist_name_col = choose_therapist_name_column(t)

    if outlet_col is None or therapist_id_col is None:
        raise ValueError("Manager action queue build failed because outlet or therapist ID column was not found.")

    if therapist_name_col is None:
        therapist_name_col = "therapist_name"
        t[therapist_name_col] = t[therapist_id_col]

    therapist_actions = pd.DataFrame({
        "outlet_name": t[outlet_col].astype(str).str.strip(),
        "therapist_id": t[therapist_id_col].astype(str).str.strip(),
        "therapist_name": t[therapist_name_col].astype(str).str.strip(),
        "queue_source": "therapist_modeled",
        "action_type": "coaching_followthrough",
        "action_owner": "Outlet Manager",
        "action_scope": "therapist",
        "priority": "medium",
        "action_title": "Therapist deployment and coaching follow-through",
        "action_rationale": pick_series(t, ["coaching_priority", "coaching_focus", "development_priority"], "Protect service consistency and strengthen therapist deployment discipline"),
        "recommended_action": pick_series(t, ["deployment_recommendation", "service_mix_recommendation", "allocation_recommendation"], "Rebalance therapist deployment to outlet service pattern and demand intensity"),
    })

    outlet_actions = pd.DataFrame({
        "outlet_name": outlet_core["outlet_name"],
        "therapist_id": "OUTLET_LEVEL",
        "therapist_name": "OUTLET_LEVEL",
        "queue_source": "outlet_modeled",
        "action_type": "staffing_structure_review",
        "action_owner": "Spa Manager",
        "action_scope": "outlet",
        "priority": outlet_core["modeled_roster_alignment_read"].astype(str).str.contains("below safe roster", case=False, na=False).map({True: "high", False: "medium"}),
        "action_title": "Outlet staffing structure review",
        "action_rationale": outlet_core["modeled_roster_alignment_read"],
        "recommended_action": "Validate outlet therapist pool against safe roster headcount, premium consistency needs, and recovery guardrails",
    })

    modeled = pd.concat([outlet_actions, therapist_actions], ignore_index=True)

    if src.empty:
        out = modeled
    else:
        s = src.copy()
        s_outlet = choose_outlet_column(s)
        s_tid = choose_therapist_id_column(s)

        if s_outlet and s_outlet != "outlet_name":
            s = s.rename(columns={s_outlet: "outlet_name"})
        if s_tid and s_tid != "therapist_id":
            s = s.rename(columns={s_tid: "therapist_id"})

        defaults = {
            "therapist_id": "",
            "therapist_name": "",
            "queue_source": "legacy_source",
            "action_type": "management_action",
            "action_owner": "Spa Manager",
            "action_scope": "outlet",
            "priority": "medium",
            "action_title": "Management action",
            "action_rationale": "",
            "recommended_action": "",
        }
        for c, default in defaults.items():
            if c not in s.columns:
                s[c] = default

        out = pd.concat([s, modeled], ignore_index=True).drop_duplicates(
            subset=["outlet_name", "therapist_id", "queue_source", "action_type", "action_title"],
            keep="last",
        )

    return out.sort_values(["outlet_name", "action_scope", "therapist_id", "action_title"]).reset_index(drop=True)


def validate_outputs(outlet_summary: pd.DataFrame, therapist_summary: pd.DataFrame, manager_queue: pd.DataFrame):
    issues = []

    if "therapist_id" in therapist_summary.columns:
        bad_ids = therapist_summary[
            ~therapist_summary["therapist_id"].astype(str).str.match(r"^[A-Z]{3}_TH_\d{3}$", na=False)
        ]
        bad_ids = bad_ids[bad_ids["therapist_id"].astype(str).ne("")]
        if not bad_ids.empty:
            issues.append(f"Non outlet-coded therapist IDs found: {bad_ids['therapist_id'].head(10).tolist()}")

    if {"outlet_name", "therapist_id"}.issubset(therapist_summary.columns):
        counts = therapist_summary.groupby("outlet_name")["therapist_id"].nunique()
        weak = counts[counts <= 1]
        if not weak.empty:
            issues.append(f"Outlets with <=1 therapist found: {weak.to_dict()}")

    if "therapist_name" in therapist_summary.columns:
        generic_hits = therapist_summary[
            therapist_summary["therapist_name"].astype(str).str.fullmatch(r"Therapist\s+[A-Z]", na=False)
        ]
        if not generic_hits.empty:
            issues.append("Generic therapist placeholder names still present")

    if "single_therapist_outlet_flag" in outlet_summary.columns:
        flagged = outlet_summary[outlet_summary["single_therapist_outlet_flag"] == True]
        if not flagged.empty:
            issues.append(f"Outlet summary still flags single-therapist outlet: {flagged['outlet_name'].tolist()}")

    if issues:
        print("\n[FAIL] validation issues detected")
        for i in issues:
            print("-", i)
        sys.exit(1)

    print("\n[OK] modeled alignment validation passed")


def main():
    therapists = read_csv_safe(THERAPIST_FP)
    roster = read_csv_safe(ROSTER_FP)

    if therapists.empty:
        raise FileNotFoundError(f"Required file missing or empty: {THERAPIST_FP}")

    print("\n=== DETECTED THERAPIST SOURCE COLUMNS ===")
    print(list(therapists.columns))

    outlet_summary_src = read_csv_safe(OUTLET_SUMMARY_SRC)
    therapist_coaching_src = read_csv_safe(THERAPIST_COACHING_SRC)
    manager_queue_src = read_csv_safe(MANAGER_QUEUE_SRC)

    outlet_core = build_outlet_core(therapists, roster)
    outlet_summary_modeled = build_outlet_management_summary_modeled(outlet_core, outlet_summary_src)
    therapist_coaching_modeled = build_therapist_coaching_summary_modeled(therapists, therapist_coaching_src)
    manager_queue_modeled = build_manager_action_queue_modeled(outlet_core, therapists, manager_queue_src)

    manager_queue_modeled["therapist_id"] = manager_queue_modeled["therapist_id"].fillna("")
    manager_queue_modeled["therapist_name"] = manager_queue_modeled["therapist_name"].fillna("")

    outlet_summary_modeled.to_csv(OUTLET_SUMMARY_OUT, index=False)
    therapist_coaching_modeled.to_csv(THERAPIST_COACHING_OUT, index=False)
    manager_queue_modeled.to_csv(MANAGER_QUEUE_OUT, index=False)

    print(f"[OK] saved: {OUTLET_SUMMARY_OUT}")
    print(f"[OK] saved: {THERAPIST_COACHING_OUT}")
    print(f"[OK] saved: {MANAGER_QUEUE_OUT}")

    validate_outputs(outlet_summary_modeled, therapist_coaching_modeled, manager_queue_modeled)

    print("\n=== THERAPIST COUNT BY OUTLET ===")
    print(
        therapist_coaching_modeled.groupby("outlet_name")["therapist_id"]
        .nunique()
        .reset_index(name="unique_modeled_therapists")
        .sort_values("outlet_name")
        .to_string(index=False)
    )

    print("\n=== SAMPLE THERAPIST IDS ===")
    print(
        therapist_coaching_modeled[["outlet_name", "therapist_id", "therapist_name"]]
        .drop_duplicates()
        .sort_values(["outlet_name", "therapist_id"])
        .head(30)
        .to_string(index=False)
    )

    print("\n=== MODELED QUEUE COUNTS ===")
    print(
        manager_queue_modeled.groupby(["queue_source", "action_type"])
        .size()
        .reset_index(name="rows")
        .sort_values(["queue_source", "rows"], ascending=[True, False])
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
