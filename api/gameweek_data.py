#!/usr/bin/env python3
"""
fetch_fpl_gw.py

Fetch per-gameweek player data from the Fantasy Premier League public API.

Outputs:
  ./output/raw/bootstrap-static.json
  ./output/raw/event_<gw>_live.json
  ./output/csv/gw_<gw>_players.csv

Usage:
  python fetch_fpl_gw.py --start 1 --end 38 --outdir ./output --sleep 1.0
"""

import requests
import time
import argparse
import os
import json
import csv
from typing import Dict, Any, List
from datetime import datetime

BASE = "https://fantasy.premierleague.com/api"

# fields we will extract from each element's stats in event live JSON
GW_STAT_FIELDS = [
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
    # sometimes other fields present (e.g. expected_goals in extended sources) but this set covers FPL standard
]

# safe HTTP request with retry
def http_get(url: str, max_retries: int = 3, timeout: int = 15):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            wait = 2 ** (attempt - 1)
            print(f"[WARN] request failed (attempt {attempt}/{max_retries}) for {url}: {e}. retrying in {wait}s")
            time.sleep(wait)
    raise last_exc

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def save_json(obj: Any, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def load_bootstrap(out_raw_dir: str) -> Dict[str, Any]:
    url = f"{BASE}/bootstrap-static/"
    print(f"[INFO] fetching bootstrap-static from {url}")
    r = http_get(url)
    data = r.json()
    save_json(data, os.path.join(out_raw_dir, "bootstrap-static.json"))
    return data

def gw_live_to_rows(gw_json: Dict[str, Any], bootstrap: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert /event/<gw>/live/ JSON into list of player-week rows.
    The live JSON has 'elements' (players) and each element has a 'stats' array describing the player's performance in that GW.
    """
    # Build maps for readable names
    players_meta = {p["id"]: p for p in bootstrap.get("elements", [])}
    teams_meta = {t["id"]: t for t in bootstrap.get("teams", [])}
    element_types = {et["id"]: et for et in bootstrap.get("element_types", [])}

    rows = []
    # event live JSON structure: {'elements': [...], 'game_id': <gw>, ...}
    gw = gw_json.get("event") or gw_json.get("gameweek") or None
    # The "elements" array in event live contains players with top-level keys:
    # id, stats (dict of stats for that gameweek), and element (player id)
    elements = gw_json.get("elements", [])
    # Some live endpoints use 'elements' with 'element' key for player id and 'stats' dict
    # Other formats might contain 'elements' with direct fields; we'll handle common shapes.
    for ele in elements:
        # Determine player_id
        player_id = ele.get("id") or ele.get("element")  # try both
        if player_id is None:
            # sometimes the structure is nested: ele = {'player': {}, 'stats': {...}} -- handle minimally
            # fallback skip unknown shape
            continue

        meta = players_meta.get(player_id, {})
        team_id = meta.get("team")
        team = teams_meta.get(team_id, {}).get("name") if team_id else None
        position_id = meta.get("element_type")
        position = element_types.get(position_id, {}).get("singular_name_short") if position_id else None

        # Extract stats
        stats_dict = ele.get("stats") or ele.get("stats", {}) or {k: ele.get(k) for k in GW_STAT_FIELDS}
        # stats_dict sometimes nested as list of dicts; handle common shapes
        if isinstance(stats_dict, list):  # unlikely but safe-guard
            # e.g. [{'identifier': 'minutes', 'value': 90}, ...]
            s = {}
            for item in stats_dict:
                if isinstance(item, dict):
                    k = item.get("identifier") or item.get("name")
                    v = item.get("value")
                    if k:
                        s[k] = v
            stats_dict = s

        # baseline row
        row = {
            "player_id": player_id,
            "player_name": meta.get("web_name") or meta.get("first_name") + " " + meta.get("second_name", ""),
            "team_id": team_id,
            "team": team,
            "position_id": position_id,
            "position": position,
            "now_cost": meta.get("now_cost"),
            "value_season": meta.get("value_season"),  # sometimes present
            "total_points_season": meta.get("total_points"),
            "selected_by_percent": meta.get("selected_by_percent"),
            "gw": gw_json.get("event") or gw,
            "fetched_at": datetime.utcnow().isoformat(),
        }

        # include GW_STAT_FIELDS, defaulting 0 if missing
        for fld in GW_STAT_FIELDS:
            # live JSON uses same names
            row[fld] = stats_dict.get(fld, 0)

        # include other useful top-level fields if present in element
        for top in ("was_home", "opponent_team", "explain", "multiplier"):
            if top in ele:
                row[top] = ele.get(top)

        rows.append(row)
    return rows

def write_rows_to_csv(rows: List[Dict[str, Any]], csv_path: str):
    if not rows:
        print(f"[WARN] no rows to write to {csv_path}")
        return
    # determine headers (union of keys)
    headers = sorted({k for r in rows for k in r.keys()})
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def fetch_and_store_gw(gw: int, out_raw_dir: str, out_csv_dir: str, bootstrap: Dict[str, Any], sleep: float = 1.0):
    url = f"{BASE}/event/{gw}/live/"
    print(f"[INFO] fetching GW {gw} from {url}")
    r = http_get(url)
    gw_json = r.json()
    raw_path = os.path.join(out_raw_dir, f"event_{gw}_live.json")
    save_json(gw_json, raw_path)
    print(f"[INFO] saved raw GW JSON to {raw_path}")

    rows = gw_live_to_rows(gw_json, bootstrap)
    csv_path = os.path.join(out_csv_dir, f"gw_{gw}_players.csv")
    write_rows_to_csv(rows, csv_path)
    print(f"[INFO] saved flattened CSV to {csv_path}")
    time.sleep(sleep)

# Optional: fetch per-player history (element-summary). Useful if you want player-week series from player side.
def fetch_element_summary(player_id: int, out_raw_dir: str):
    url = f"{BASE}/element-summary/{player_id}/"
    r = http_get(url)
    data = r.json()
    save_json(data, os.path.join(out_raw_dir, f"element_{player_id}_summary.json"))
    return data

def main():
    p = argparse.ArgumentParser(description="Fetch per-gameweek player data from FPL API")
    p.add_argument("--start", type=int, default=1, help="start gameweek (inclusive)")
    p.add_argument("--end", type=int, default=38, help="end gameweek (inclusive)")
    p.add_argument("--outdir", type=str, default="./output", help="output directory")
    p.add_argument("--sleep", type=float, default=1.0, help="sleep seconds between requests")
    p.add_argument("--fetch-player-history", action="store_true", help="also fetch element-summary for each player (slower)")
    args = p.parse_args()

    outdir = args.outdir
    raw_dir = os.path.join(outdir, "raw")
    csv_dir = os.path.join(outdir, "csv")
    ensure_dir(raw_dir)
    ensure_dir(csv_dir)

    bootstrap = load_bootstrap(raw_dir)
    # if you want to limit gws to ones actually in season, you can read bootstrap['events'] to find available gameweeks
    events = bootstrap.get("events", [])
    max_event = max((e.get("id", 0) for e in events), default=38)
    print(f"[INFO] detected max_event = {max_event} from bootstrap (requested end={args.end})")

    start_gw = max(1, args.start)
    end_gw = min(args.end, max_event)

    for gw in range(start_gw, end_gw + 1):
        try:
            fetch_and_store_gw(gw, raw_dir, csv_dir, bootstrap, sleep=args.sleep)
        except Exception as e:
            print(f"[ERROR] failed to fetch or store GW {gw}: {e}")

    if args.fetch_player_history:
        # fetch element-summary for each player (slower)
        player_ids = [p["id"] for p in bootstrap.get("elements", [])]
        print(f"[INFO] fetching element-summary for {len(player_ids)} players (this will take a while)")
        for pid in player_ids:
            try:
                fetch_element_summary(pid, raw_dir)
                time.sleep(0.5)
            except Exception as e:
                print(f"[WARN] failed to fetch element-summary for player {pid}: {e}")

    print("[DONE] all requested gameweeks processed.")

if __name__ == "__main__":
    main()
