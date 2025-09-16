from pathlib import Path
import pandas as pd


data_path = Path("~/MBAI/data").expanduser()

def season_path(season_year): 
    return data_path / f"rs2{season_year}"

def game_path(season_year, game_id): 
    return season_path(season_year) / f"g{game_id}"


def load_schedule(season_year):
    season_dir = season_path(season_year)
    schedule_df = pd.read_parquet(season_dir / "schedule.parquet")
    return schedule_df


def load_teams_gamelogs(season_year):
    season_dir = season_path(season_year)
    
    dfs = [
        pd.read_parquet(game_dir / "teams_gamelog.parquet")
            for game_dir in season_dir.iterdir()
                if game_dir.is_dir() and game_dir.name.startswith('g')
    ]

    return pd.concat(dfs).reset_index(drop=True)


def load_players_gamelogs(season_year):
    season_dir = season_path(season_year)
    
    dfs = [
        pd.read_parquet(game_dir / "players_gamelog.parquet")
            for game_dir in season_dir.iterdir()
                if game_dir.is_dir() and game_dir.name.startswith('g')
    ]

    return pd.concat(dfs).reset_index(drop=True)