import pandas as pd
from pathlib import Path
from tqdm import tqdm

from nba_api.stats.endpoints import LeagueGameLog
from nba_api.live.nba.endpoints import PlayByPlay


def get_leaguegamelog_dfs(year) -> pd.DataFrame, pd.DataFrame:

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

    df_T['GAME_ID'] = df_T['GAME_ID'].astype('string')
    df_P['GAME_ID'] = df_P['GAME_ID'].astype('string')

    # convert 'WL' to bool
    df_T['WL'] = df_T['WL'].replace({'W': True, 'L': False})
    df_P['WL'] = df_P['WL'].replace({'W': True, 'L': False})

    # downcast teams stats
    for col in df_T.select_dtypes(include=['int64']).columns:
        df_T[col] = pd.to_numeric(df_T[col], downcast='unsigned')

    # downcast players stats
    for col in df_P.select_dtypes(include=['int64']).columns:
        df_P[col] = pd.to_numeric(df_P[col], downcast='unsigned')
    
    df_P['PLUS_MINUS'] = pd.to_numeric(df_P['PLUS_MINUS'], downcast='signed')

    return df_T, df_P


def get_playbyplay_df(game_id) -> pd.DataFrame:
    pbp = PlayByPlay(game_id)

    cols2drop = [
        'actionNumber', 'periodType', 'edited', 'orderNumber',
        'isTargetScoreLastPeriod', 'isFieldGoal', 'side', 'personIdsFilter',
        'teamTricode', 'jumpBallRecoveredName', 'playerName',
        'playerNameI', 'jumpBallWonPlayerName', 'jumpBallLostPlayerName', 
        'blockPlayerName', 'shotActionNumber', 'reboundTotal', 'reboundDefensiveTotal',
        'reboundOffensiveTotal', 'pointsTotal', 'assistPlayerNameInitial',
        'assistTotal', 'foulPersonalTotal', 'foulTechnicalTotal', 
        'foulDrawnPlayerName', 'turnoverTotal', 'stealPlayerName',
        'qualifiers', 'description', 'xLegacy', 'yLegacy', 'area', 'areaDetail'
    ]

    df = pd.DataFrame(pbp.get_dict()['game']['actions']).drop(columns=cols2drop)

    df['clock'] = pd.to_timedelta(df['clock'])
    df['timeActual'] = pd.to_datetime(df['timeActual'])
    
    df['actionType'] = df['actionType'].astype('string') 
    df['subType'] = df['subType'].astype('string') 
    df['descriptor'] = df['descriptor'].astype('string') 
    
    df['x'] = df['x'].astype('float16')
    df['y'] = df['y'].astype('float16')
    
    df['scoreHome'] = df['scoreHome'].astype('uint8')
    df['scoreAway'] = df['scoreAway'].astype('uint8')

    df['shotResult'] = df['shotResult'].replace({'Made': True, 'Missed': False}).astype('boolean')
    df['shotDistance'] = df['shotDistance'].astype('float16')

    df['personId'] = df['personId'].replace(0, pd.NA).astype('UInt32')
    df['possession'] = df['possession'].replace(0, pd.NA).astype('uint32')
    df['teamId'] = df['teamId'].astype('UInt32')
        
    person_id_cols = df.filter(regex='PersonId$').columns
    df[person_id_cols] = df[person_id_cols].astype('UInt32')

    return df


def load_regular_season_data(year):
    df_T, df_P = get_gamelogs(year)
    
    data_path = Path("~/MBAI/data").expanduser()
    season_path = data_path / f"rs{season_id}"
    
    for game_id in tqdm(game_ids):
        pbp_df = get_playbyplay_df(game_id)

        game_path = season_path / f"g{game_id}"
        
        gamelogs_path = game_path / "gamelogs"        
        try: 
            gamelogs_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error in creating the gamelogs directory: {e}")
    
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

        playbyplay_path = game_path / "playbyplay"
        try: 
            playbyplay_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error in creating the playbyplay directory: {e}")

        try:
            pbp_df.to_csv(playbyplay_path / "actions.csv")
        except Exception as e:
            print(f"Error in saving the actions csv: {e}")
