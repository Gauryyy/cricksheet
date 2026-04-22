import json
import os
from glob import glob
import pandas as pd


def transform_data(extract_path, logger):
    # Combine files from base extract and admin uploads
    # Using recursive=True to handle cases where files are nested in subfolders after re-zipping
    base_files = glob(os.path.join(extract_path, "**", "*.json"), recursive=True)
    new_data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'new')
    new_files = glob(os.path.join(new_data_path, "**", "*.json"), recursive=True) if os.path.exists(new_data_path) else []
    
    logger.info(f"TRANSFORM | SOURCE: Base Dataset | COUNT: {len(base_files)}")
    logger.info(f"TRANSFORM | SOURCE: Admin Uploads | COUNT: {len(new_files)}")
    
    files = list(set(base_files + new_files)) # Deduplicate by path
    
    if not files:
        logger.warning("TRANSFORM | No JSON files found in source directories!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    matches = []
    deliveries = []

    logger.info(f"TRANSFORM | TOTAL_FILES_QUEUED: {len(files)}")

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        info = data.get("info", {})
        match_id = os.path.basename(file).replace(".json", "")

        # Match table
        matches.append({
            "match_id": match_id,
            "date": (info.get("dates") or [None])[0],
            "team1": (info.get("teams") or [None, None])[0],
            "team2": (info.get("teams") or [None, None])[1],
            "venue": info.get("venue"),
            "city": info.get("city"),
            "winner": info.get("outcome", {}).get("winner"),
            "toss_winner": info.get("toss", {}).get("winner"),
            "toss_decision": info.get("toss", {}).get("decision"),
        })

        # Deliveries table
        for i, innings in enumerate(data.get("innings", [])):

            # ✅ Handle BOTH formats (new + old)
            if isinstance(innings, dict) and "team" in innings:
                inning_data = innings
            else:
                # old format: {"1st innings": {...}}
                inning_data = list(innings.values())[0]

            team = inning_data.get("team")

            for over_data in inning_data.get("overs", []):
                over = over_data.get("over")

                for ball_idx, delivery in enumerate(over_data.get("deliveries", [])):
                    wicket = delivery.get("wickets", [{}])[0]
                    deliveries.append({
                        "match_id": match_id,
                        "innings": i + 1,
                        "batting_team": team,
                        "over": over,
                        "ball": ball_idx + 1,
                        "batter": delivery.get("batter"),
                        "bowler": delivery.get("bowler"),
                        "runs_total": delivery.get("runs", {}).get("total"),
                        "wicket_kind": wicket.get("kind"),
                        "player_out": wicket.get("player_out"),
                    })

    logger.info("Transformation complete")

    # Convert to DataFrames for aggregation
    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    if deliveries_df.empty:
        logger.warning("TRANSFORM | No delivery data extracted from found files.")
        return matches_df, deliveries_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Calculate Player Stats
    player_stats = deliveries_df.groupby("batter").agg({
        "match_id": "nunique",
        "runs_total": ["sum", "count"]
    }).reset_index()
    player_stats.columns = ["player", "matches", "runs", "balls_faced"]
    
    # Calculate strike rate, average, fours, sixes
    player_stats["strike_rate"] = (player_stats["runs"] / player_stats["balls_faced"] * 100).round(2)
    player_stats["average"] = (player_stats["runs"] / player_stats["matches"]).round(2)
    
    # Count fours and sixes
    fours_sixes = deliveries_df[deliveries_df["runs_total"].isin([4, 6])].groupby(["batter", "runs_total"]).size().unstack(fill_value=0).reset_index()
    fours_sixes.columns = ["player", "fours", "sixes"] if 6 in fours_sixes.columns else ["player", "fours"]
    if "sixes" not in fours_sixes.columns:
        fours_sixes["sixes"] = 0
    if "fours" not in fours_sixes.columns:
        fours_sixes["fours"] = 0
    
    player_stats = player_stats.merge(fours_sixes[["player", "fours", "sixes"]], on="player", how="left").fillna(0)
    player_stats[["fours", "sixes", "matches"]] = player_stats[["fours", "sixes", "matches"]].astype(int)
    player_stats["innings"] = player_stats["matches"]  # Using matches as proxy for innings
    player_stats = player_stats[["player", "matches", "innings", "runs", "balls_faced", "strike_rate", "average", "fours", "sixes"]]

    # Calculate Team Stats
    team_stats_list = []
    for team in pd.concat([matches_df["team1"], matches_df["team2"]]).unique():
        team_matches = matches_df[(matches_df["team1"] == team) | (matches_df["team2"] == team)]
        matches_count = len(team_matches)
        wins = (team_matches["winner"] == team).sum()
        losses = matches_count - wins
        win_percentage = (wins / matches_count * 100).round(2) if matches_count > 0 else 0
        
        # Calculate average scores
        team_deliveries = deliveries_df[deliveries_df["batting_team"] == team]
        avg_score = (team_deliveries["runs_total"].sum() / matches_count).round(2) if matches_count > 0 else 0
        
        # Average conceded (runs by opponent)
        opponent_deliveries = deliveries_df[
            (deliveries_df["match_id"].isin(team_matches["match_id"])) & 
            (deliveries_df["batting_team"] != team)
        ]
        avg_conceded = (opponent_deliveries["runs_total"].sum() / matches_count).round(2) if matches_count > 0 else 0
        
        team_stats_list.append({
            "team": team,
            "matches": matches_count,
            "wins": int(wins),
            "losses": int(losses),
            "win_percentage": win_percentage,
            "avg_score": avg_score,
            "avg_conceded": avg_conceded
        })
    
    team_stats = pd.DataFrame(team_stats_list)

    # Calculate Match Summary
    match_summary_list = []
    for _, match in matches_df.iterrows():
        match_id = match["match_id"]
        team1 = match["team1"]
        team2 = match["team2"]
        
        # Get scores for each team
        team1_score = deliveries_df[(deliveries_df["match_id"] == match_id) & (deliveries_df["batting_team"] == team1)]["runs_total"].sum()
        team2_score = deliveries_df[(deliveries_df["match_id"] == match_id) & (deliveries_df["batting_team"] == team2)]["runs_total"].sum()
        
        # Calculate margin (simplified)
        margin = abs(team1_score - team2_score)
        
        match_summary_list.append({
            "match_id": match_id,
            "team1_score": int(team1_score),
            "team2_score": int(team2_score),
            "winner": match["winner"],
            "margin": int(margin),
            "toss_winner": match["toss_winner"],
            "toss_decision": match["toss_decision"]
        })
    
    match_summary = pd.DataFrame(match_summary_list)

    logger.info(f"TRANSFORM | Derived tables successfully calculated")
    logger.info(f"TRANSFORM | STATS | Players: {len(player_stats)} | Teams: {len(team_stats)} | Match Summaries: {len(match_summary)}")

    return matches, deliveries, player_stats, team_stats, match_summary