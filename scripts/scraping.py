import pandas as pd
import logging
import logging.handlers
from pathlib import Path
from time import time, sleep
from datetime import datetime
from dataclasses import dataclass

from nba_api.stats.endpoints import LeagueGameLog
from nba_api.live.nba.endpoints import PlayByPlay


def data_path():
    return Path("~/MBAI/data").expanduser()

def season_path(season_id): 
    return data_path() / f"rs{season_id}"

def game_path(season_id, game_id): 
    return season_path(season_id) / f"g{game_id}"


@dataclass
class NBAScraperConfig:
    """Configuration for NBA API scraping"""
    max_retries: int = 3
    delay_between_requests: float = 5.0
    max_log_size_mb: int = 10
    backup_count: int = 3


class NBAScraper:
    def __init__(self, config=NBAScraperConfig()):
        self.config = config
        self.logger = self.get_root_logger()
        
    def set_max_retries(self, max_retries):
        self.config.max_retries = max_retries

    def set_delay_between_requests(self, delay):
        self.config.delay_between_requests = delay
    
    def api_call(self, logger, api_fn):
        """Simple retry mechanism for API calls using config settings"""
        for attempt in range(self.config.max_retries):
            try: 
                return api_fn()
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    logger.warning(f"⚠️ All {self.config.max_retries} attempts failed: {str(e)}")
                    return None

                logger.warning(f"⚠️ Attempt {attempt + 1}/{self.config.max_retries} failed: {str(e)}")
                sleep(self.config.delay_between_requests)

    
    def get_root_logger(self) -> logging.Logger:
        logger = logging.getLogger('NBAScraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    
    def get_season_logger(self, season_id) -> logging.Logger:
        logger = logging.getLogger(f'NBAScraper.rs{season_id}')
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if logger.handlers:
            return logger

        logs_dir = season_path(season_id) / "logs"
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Unexpected error creating logs directory: {str(e)}")
                    
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = f"scraping_{timestamp}.log"
        error_log_file = f"errors_{timestamp}.log"

        file_handler = logging.handlers.RotatingFileHandler(
            logs_dir / log_file,
            maxBytes=self.config.max_log_size_mb * 1024**2,
            backupCount=self.config.backup_count
        )
    
        error_handler = logging.handlers.RotatingFileHandler(
            logs_dir / error_log_file,
            maxBytes=self.config.max_log_size_mb * 1024**2,
            backupCount=self.config.backup_count
        )
        error_handler.setLevel(logging.ERROR)

        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        file_handler.setFormatter(detailed_formatter)
        error_handler.setFormatter(detailed_formatter)
        console_handler.setFormatter(formatter)
                
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.addHandler(error_handler)
        
        self.logger.info(f"Initialized logger for season {season_id}")
        
        return logger

    def get_game_logger(self, season_id, game_id) -> logging.Logger:
        season_logger = self.get_season_logger(season_id)
        
        logger = logging.getLogger(f'NBAScraper.rs{season_id}.g{game_id}')
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if logger.handlers:
            return logger

        logs_dir = game_path(season_id, game_id) / "logs"
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Unexpected error creating logs directory: {str(e)}")
                    
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = f"scraping_{timestamp}.log"
        error_log_file = f"errors_{timestamp}.log"

        file_handler = logging.handlers.RotatingFileHandler(
            logs_dir / log_file,
            maxBytes=self.config.max_log_size_mb * 1024**2,
            backupCount=self.config.backup_count
        )
    
        error_handler = logging.handlers.RotatingFileHandler(
            logs_dir / error_log_file,
            maxBytes=self.config.max_log_size_mb * 1024**2,
            backupCount=self.config.backup_count
        )
        error_handler.setLevel(logging.ERROR)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        file_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
                
        logger.addHandler(file_handler)
        logger.addHandler(error_handler)
                
        return logger

        
    def scrape_season(self, starting_year):        
        start_time = time()

        season_id = f"2{starting_year}"
        season_dir = season_path(season_id)
        try:
            season_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"❌ Unexpected error in creating season directory: {str(e)}")
        
        logger = self.get_season_logger(season_id)
        
        game_ids_from_team = []        
        logger.info(f"🚀 Fetching team gamelogs.")
        raw_team_gamelogs = self.api_call(logger, lambda: LeagueGameLog(season=starting_year, player_or_team_abbreviation='T'))
        if raw_team_gamelogs is None:
            logger.error("❌ Error in fetching team gamelogs.")
        else: 
            try: 
                df = raw_team_gamelogs.get_data_frames()[0]
                game_ids_from_team = df['GAME_ID'].unique().tolist()
                df.to_csv(season_dir / "raw_team_gamelogs.csv", index=False)
            except Exception as e:
                logger.error(f"❌ Error in saving team gamelogs: {str(e)}")

        game_ids_from_player = []
        logger.info(f"🚀 Fetching player gamelogs.")
        raw_player_gamelogs = self.api_call(logger, lambda: LeagueGameLog(season=starting_year, player_or_team_abbreviation='P'))
        if raw_player_gamelogs is None:
            logger.error(f"❌ Error in fetching player gamelogs.")
        else: 
            try:
                df = raw_player_gamelogs.get_data_frames()[0]
                game_ids_from_player = df['GAME_ID'].unique().tolist()
                df.to_csv(season_dir / "raw_player_gamelogs.csv", index=False)
            except Exception as e:
                logger.error(f"❌ Error in saving player gamelogs: {str(e)}.")

        if set(game_ids_from_team) != set(game_ids_from_player): 
            logger.error("❌ Error: GAME_ID mismatches between team and player gamelogs. Game actions will not be scraped!")
            return None
        else:
            total_games = len(game_ids_from_team)
            logger.info(f"🚀 Fetching game actions for {total_games} games.")
            for i, game_id in enumerate(game_ids_from_team, 1):
                if i % 100 == 0: logger.info(f"📊 Progress: {i}/{total_games} games processed")
                self.scrape_game(season_id, game_id)
            
        logger.info(f"✅ Successfully processed season data in {time() - start_time:.2f}s")


    def scrape_game(self, season_id, game_id):
        start_time = time()
        season_logger = self.get_season_logger(season_id)
        logger = self.get_game_logger(season_id, game_id)

        game_dir = game_path(season_id, game_id)
        try:
            game_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            season_logger.error(f"❌ Unexpected error in creating game directory: {str(e)}")
        
        logger.info(f"🚀 Fetching game actions.")
        raw_game_actions = self.api_call(logger, lambda: PlayByPlay(game_id))  
        if raw_game_actions is None:
            season_logger.error(f"❌ Error in fetching game actions for {game_id}.")
        else: 
            try: 
                df = pd.DataFrame(raw_game_actions.get_dict()['game']['actions'])
                df.to_csv(game_dir / "raw_actions.csv", index=False)
            except Exception as e:
                logger.error(f"❌ Error in saving game actions: {str(e)}.")

        logger.info(f"✅ Successfully processed game data in {time() - start_time:.2f}s")
