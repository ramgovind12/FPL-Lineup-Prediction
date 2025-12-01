#!/usr/bin/env python3
"""
get_gw_players_2025_26.py

Fetch gameweek-wise player data for the 2025-26 FPL season and flatten to CSV per gameweek.

Behavior:
 - If the official FPL API reports current season == "2025-26", fetch /event/<gw>/live/ from API.
 - Otherwise (historical season), clone the Vaastav FPL repo (or alternate repo) and attempt to use its saved CSVs.
 - Flatten outputs into ./output/2025-26/csv/gw_<gw>_players.csv and keep raw JSONs in ./output/2025-26/raw/

Usage:
    python get_gw_players_2025_26.py
    (or) python get_gw_players_2025_26.py --repo "https://github.com/vaastav/Fantasy-Premier-League.git"

Outputs:
 - ./output/2025-26/raw/bootstrap-static.json
 - ./output/2025-26/raw/event_<gw>_live.json  (if from API)
 - ./output/2025-26/csv/gw_<gw>_players.csv
"""

import os
import json
import time
import requests
import shutil
import subprocess
import csv
from datetime import datetime
from typing import Dict, Any, List
import argparse

BASE = "https://fantasy.premierleague.com/api"
TARGET_SEASON = "2025-26"
DEFAULT_REPO = "https://github.com/vaastav/Fantasy-Premier-League.git"
CLONE_DIR = "./_fpl_repo_clone"

# canonical fields to include in each GW CSV
GW_FIELDS = [
    "gw",
    "player_id",
    "player_name",
    "team_id",
    "team",
    "position_id",
    "position",
    "now_cost",
    "total_points_season",
    "selected_by_percent",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "total_points",
    "fetched_at",
]

# simple http get with retries
def http_get(url: str, max_retries: int = 3, timeout: int = 20):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            wait = 2 ** (attempt - 1)
            print(f"[WARN] request failed (attempt {attempt}/{max_retries}) for {url}: {e}; retrying in {wait}s")
            time.sleep(wait)
    raise last_exc

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def save_json(obj: Any, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_bootstrap(out_raw_dir: str) -> Dict[str, Any]:
    url = f"{BASE}/bootstrap-static/"
    print(f"[INFO] fetching bootstrap-static from FPL API")
    r = http_get(url)
    data = r.json()
    save_json(data, os.path.join(out_raw_dir, "bootstrap-static.json"))
    return data

def fetch_event_live(gw: int, out_raw_dir: str) -> Dict[str, Any]:
    url = f"{BASE}/event/{gw}/live/"
    print(f"[INFO] fetching event {gw} live from API")
    r = http_get(url)
    data = r.json()
    path = os.path.join(out_raw_dir, f"event_{gw}_live.json")
    save_json(data, path)
    return data

def flatten_live_to_rows(gw_json: Dict[str, Any], bootstrap: Dict[str, Any]) -> List[Dict[str, Any]]:
    # build lookup maps
    players_meta = {p["id"]: p for p in bootstrap.get("elements", [])}
    teams_meta = {t["id"]: t for t in bootstrap.get("teams", [])}
    element_types = {et["id"]: et for et in bootstrap.get("element_types", [])}

    rows = []
    elements = gw_json.get("elements", [])  # common shape for /event/<gw>/live/
    gw_id = gw_json.get("event") or gw_json.get("gameweek") or None
    for e in elements:
        player_id = e.get("id") or e.get("element")
        if player_id is None:
            # try nested shapes (skip if unrecognized)
            continue
        meta = players_meta.get(player_id, {})
        team_id = meta.get("team")
        team_name = teams_meta.get(team_id, {}).get("name") if team_id else None
        pos_id = meta.get("element_type")
        pos_name = element_types.get(pos_id, {}).get("singular_name_short") if pos_id else None

        stats = e.get("stats") or {}
        # stats might be a dict; make sure missing keys return 0
        row = {
            "gw": gw_id,
            "player_id": player_id,
            "player_name": meta.get("web_name") or (meta.get("first_name", "") + " " + meta.get("second_name", "")),
            "team_id": team_id,
            "team": team_name,
            "position_id": pos_id,
            "position": pos_name,
            "now_cost": meta.get("now_cost"),
            "total_points_season": meta.get("total_points"),
            "selected_by_percent": meta.get("selected_by_percent"),
            "fetched_at": datetime.utcnow().isoformat(),
        }
        # add canonical stat fields with default 0
        for fld in GW_FIELDS:
            if fld in ("gw", "player_id", "player_name", "team_id", "team",
                       "position_id", "position", "now_cost", "total_points_season",
                       "selected_by_percent", "fetched_at"):
                continue
            row[fld] = stats.get(fld, 0)
        rows.append(row)
    return rows

def write_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        print(f"[WARN] no rows to write for {csv_path}")
        return
    ensure_dir(os.path.dirname(csv_path))
    # ensure header order
    headers = GW_FIELDS
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            # ensure all headers present
            out = {h: r.get(h, 0) for h in headers}
            writer.writerow(out)

def clone_repo(repo_url: str, dest: str):
    if os.path.exists(dest):
        print(f"[INFO] repo already cloned at {dest}")
        return
    print(f"[INFO] cloning {repo_url} into {dest}")
    subprocess.check_call(["git", "clone", "--depth", "1", repo_url, dest])

def find_and_copy_season_from_clone(clone_dir: str, season_str: str, dest_dir: str) -> bool:
    """
    Look for plausible season folders in cloned repo and copy them to dest_dir.
    Returns True if copied, False otherwise.
    """
    candidates = [
        os.path.join(clone_dir, "data", season_str),
        os.path.join(clone_dir, "data", season_str.replace("-", "")),
        os.path.join(clone_dir, "season", season_str),
        os.path.join(clone_dir, season_str),
        os.path.join(clone_dir, "seasons", season_str),
    ]
    for cand in candidates:
        if os.path.exists(cand):
            print(f"[INFO] found season content at {cand}; copying to {dest_dir}")
            if os.path.exists(dest_dir):
                shutil.rmtree(dest_dir)
            shutil.copytree(cand, dest_dir)
            return True
    return False

def flatten_vaastav_gws(season_folder: str, out_csv_dir: str):
    """
    Vaastav repo layout often contains per-GW CSVs under 'data/<season>/gws' or similar.
    We'll search for CSV files that look like gw_*.csv and copy/normalize them.
    """
    # search for gw CSVs recursively
    found = False
    for root, _, files in os.walk(season_folder):
        for fname in files:
            if fname.lower().startswith("gw_") and fname.lower().endswith(".csv"):
                found = True
                src = os.path.join(root, fname)
                # read and transform minimally: ensure it has expected columns or rename plausible ones
                print(f"[INFO] processing {src}")
                out_name = os.path.join(out_csv_dir, fname)
                ensure_dir(out_csv_dir)
                shutil.copy(src, out_name)
    if not found:
        print("[WARN] no 'gw_*.csv' files found in cloned season folder.")
    return found

def main(repo: str):
    out_base = os.path.abspath("./output")
    season_dir = os.path.join(out_base, TARGET_SEASON)
    raw_dir = os.path.join(season_dir, "raw")
    csv_dir = os.path.join(season_dir, "csv")
    ensure_dir(raw_dir)
    ensure_dir(csv_dir)

    # attempt to fetch bootstrap from API
    try:
        bootstrap = fetch_bootstrap(raw_dir)
    except Exception as e:
        print(f"[WARN] could not fetch bootstrap from API: {e}")
        bootstrap = None

    api_season = None
    if bootstrap:
        api_season = bootstrap.get("season_name") or bootstrap.get("season")
        if isinstance(api_season, str):
            api_season = api_season.replace("/", "-").strip()
        print(f"[INFO] API season detected as: {api_season}")

    if api_season == TARGET_SEASON:
        # fetch all gameweeks from API events list
        events = bootstrap.get("events", []) if bootstrap else []
        gw_ids = sorted([e.get("id") for e in events if e.get("id") is not None])
        if not gw_ids:
            print("[WARN] no events in bootstrap; defaulting to 1..38")
            gw_ids = list(range(1, 39))
        print(f"[INFO] will fetch GWs: {gw_ids} from API")
        for gw in gw_ids:
            try:
                gw_json = fetch_event_live(gw, raw_dir)
                rows = flatten_live_to_rows(gw_json, bootstrap)
                csv_path = os.path.join(csv_dir, f"gw_{gw}_players.csv")
                write_csv(rows, csv_path)
                print(f"[INFO] written {csv_path} ({len(rows)} rows)")
                time.sleep(0.5)
            except Exception as e:
                print(f"[ERROR] failed on gw {gw}: {e}")
        print(f"[DONE] API-based fetch complete. CSVs in: {csv_dir}")
        return

    # Otherwise, try to fetch historical season from community repo
    print(f"[INFO] API season != target ({TARGET_SEASON}). Attempting to clone and extract from repo: {repo}")
    try:
        clone_repo(repo, CLONE_DIR)
        season_src = os.path.join(CLONE_DIR, TARGET_SEASON)
        copied = find_and_copy_season_from_clone(CLONE_DIR, TARGET_SEASON, season_src)
        if not copied:
            # try to copy root/data/<season> etc handled in function
            # if still not found, just attempt to search for any folder containing 'gws' or 'gw_' CSVs
            print("[INFO] attempting to find GW CSVs anywhere in cloned repo")
            # fallback: flatten any gw CSVs found by recursive scan and copy to output
            flattened = flatten_vaastav_gws(CLONE_DIR, csv_dir)
            if flattened:
                print(f"[DONE] copied found GW CSVs into {csv_dir}")
                return
            else:
                raise FileNotFoundError("Could not find season GW CSVs in cloned repo.")
        else:
            # if copy successful, attempt to flatten known folders inside the copied tree
            # common structure: <season>/gws/*.csv or <season>/data/gws
            candidate_gws = [
                os.path.join(season_src, "gws"),
                os.path.join(season_src, "data", "gws"),
                os.path.join(season_src, "gw"),
            ]
            found_any = False
            for cand in candidate_gws:
                if os.path.exists(cand):
                    print(f"[INFO] flattening GW CSVs from {cand}")
                    for fname in sorted(os.listdir(cand)):
                        if fname.lower().endswith(".csv") and "gw" in fname.lower():
                            src = os.path.join(cand, fname)
                            dst = os.path.join(csv_dir, fname if fname.lower().endswith(".csv") else f"{fname}.csv")
                            shutil.copy(src, dst)
                            print(f"[INFO] copied {src} -> {dst}")
                            found_any = True
            if not found_any:
                # attempt generic flatten
                flattened = flatten_vaastav_gws(season_src, csv_dir)
                if not flattened:
                    raise FileNotFoundError("No GW CSVs found inside copied season folder.")
            print(f"[DONE] historical-season fetch complete. CSVs in: {csv_dir}")
            return
    except Exception as e:
        print(f"[ERROR] failed to fetch historical season data: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch gameweek-wise player data for 2025-26")
    parser.add_argument("--repo", type=str, default=DEFAULT_REPO, help="Fallback repo for historical seasons (Vaastav recommended)")
    args = parser.parse_args()
    main(args.repo)
