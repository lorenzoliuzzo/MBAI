import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog
from pathlib import Path
from tqdm import tqdm


def save_gamelogs_from_year(year):
    # use nba_api to scrape league gamelogs for teams and players
    df_T = LeagueGameLog(season=year, player_or_team_abbreviation='T').get_data_frames()[0]
    assert df_T['TEAM_ID'].nunique() == 30, f"The NBA league is composed of 30 teams, but we're getting {df_T['TEAM_ID'].nunique()}!"

    df_P = LeagueGameLog(season=year, player_or_team_abbreviation='P').get_data_frames()[0]

    assert df_T['SEASON_ID'].unique() == df_P['SEASON_ID'].unique(), "SEASON_ID mismatch!"
    season_id = df_T['SEASON_ID'].unique()[0]
    
    assert set(df_T['GAME_ID']) == set(df_P['GAME_ID']), "GAME_ID mismatches!"
    game_ids = df_T['GAME_ID'].unique()

    # basic asserts on baskeball rules
    assert df_T['WL'].nunique() == df_P['WL'].nunique() == 2, "A basketball game can only finish with a win or a lose!"
    
    for game_id in game_ids:
        teams_df = df_T[df_T['GAME_ID'] == game_id]
        assert len(teams_df) == 2, f"Game {game_id} has {len(teams_df)} teams instead of 2!"
        
        players_df = df_P[df_P['GAME_ID'] == game_id]
        for team_id in teams_df['TEAM_ID']:
            assert len(players_df[players_df['TEAM_ID'] == team_id]) >= 5, f"Game {game_id}, Team {team_id} has only {len(team_players)} players"

    
    # check for game counts for each team
    game_counts = df_T.groupby('TEAM_ID').size()
    assert game_counts.nunique() == 1, "Teams have different game counts!"
    assert game_counts.unique() == 82, f"An NBA regular season is composed of 82 games, but we're getting {game_counts.unique()[0]}!"


    # remove unuseful columns from dfs
    cols2drop = ['SEASON_ID', 'GAME_DATE', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'MATCHUP', 'VIDEO_AVAILABLE', 'FG_PCT', 'FG3_PCT', 'FT_PCT']
    df_T.drop(columns=cols2drop + ['PLUS_MINUS'], inplace=True)
    df_P.drop(columns=cols2drop + ['TEAM_ID', 'PLAYER_NAME', 'FANTASY_PTS'], inplace=True)

    # convert 'WL' to boolean
    df_T['WL'] = df_T['WL'].replace({'W': 1, 'L': 0}).astype('bool')
    df_P['WL'] = df_P['WL'].replace({'W': 1, 'L': 0}).astype('bool')

    # downcast teams stats
    for col in df_T.select_dtypes(include=['int64']).columns:
        df_T[col] = pd.to_numeric(df_T[col], downcast='unsigned')

    # downcast players stats
    for col in df_P.select_dtypes(include=['int64']).columns:
        df_P[col] = pd.to_numeric(df_P[col], downcast='unsigned')
    
    df_P['PLUS_MINUS'] = pd.to_numeric(df_P['PLUS_MINUS'], downcast='signed')
    
    # save dfs as csv in the data directory
    data_path = Path("~/MBAI/data").expanduser()
    season_path = data_path / f"rs{season_id}"
    for game_id in tqdm(game_ids):
        gamelogs_path = season_path / f"g{game_id}" / "gamelogs" 
        try: 
            gamelogs_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error in creating the game directory: {e}")
    
        teams_df = df_T[df_T['GAME_ID'] == game_id].drop('GAME_ID', axis=1)
        try:
            teams_df.to_csv(gamelogs_path / "teams.csv", index=False)
        except Exception as e:
            print(f"Error in saving the teams csv: {e}")
    
        players_df = df_P[df_P['GAME_ID'] == game_id].drop('GAME_ID', axis=1)
        try:
            players_df.to_csv(gamelogs_path / "players.csv", index=False)
        except Exception as e:
            print(f"Error in saving the players csv: {e}")