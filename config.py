# config.py
"""
Centralized configuration management for LoL Analytics Application.
All environment variables and configuration constants are defined here.
"""

import os
import json
from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database configuration"""
    # Fix: Use relative path joining correctly
    base_dir: str = field(default_factory=lambda: os.path.abspath(os.path.dirname(__file__)))

    @property
    def database_path(self) -> str:
        """Get the correct database path"""
        # FIXED: Correct path joining without leading slash
        db_path = os.path.join(self.base_dir, 'data', 'scrims_data.db')

        # Ensure the data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        return db_path


@dataclass
class APIConfig:
    """API keys and endpoints configuration"""
    grid_api_key: str = field(default_factory=lambda: os.getenv("GRID_API_KEY", ""))
    riot_api_key: str = field(default_factory=lambda: os.getenv("RIOT_API_KEY", ""))

    # API Configuration
    grid_base_url: str = "https://api.grid.gg/"
    api_request_delay: float = 0.5  # Seconds between requests

    def validate(self) -> bool:
        """Validate that required API keys are present"""
        if not self.grid_api_key:
            raise ValueError("GRID_API_KEY environment variable is not set")
        return True


@dataclass
class TournamentConfig:
    """Tournament-specific configuration"""
    tournament_id: str = field(default_factory=lambda: os.getenv("TOURNAMENT_ID", "827201"))
    tournament_name: str = field(default_factory=lambda: os.getenv("TOURNAMENT_NAME", "HLL Split 3"))
    match_start_date_filter: str = "2025-04-03T00:00:00Z"

    # Team mappings file path
    _team_mappings_cache: Optional[Dict[str, str]] = None

    @property
    def team_mappings_file(self) -> str:
        """Path to team mappings JSON file"""
        base_dir = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(base_dir, 'config', 'config/team_mappings.json')

    @property
    def team_tag_to_full_name(self) -> Dict[str, str]:
        """Load team mappings from JSON file with caching"""
        if self._team_mappings_cache is not None:
            return self._team_mappings_cache

        # Ensure config directory exists
        os.makedirs(os.path.dirname(self.team_mappings_file), exist_ok=True)

        # Try to load from file
        if os.path.exists(self.team_mappings_file):
            try:
                with open(self.team_mappings_file, 'r', encoding='utf-8') as f:
                    self._team_mappings_cache = json.load(f)
                    return self._team_mappings_cache
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load team mappings: {e}")

        # Return default mappings if file doesn't exist
        default_mappings = {
            "GSMC": "Gamespace MC",
            # Add more default mappings as needed
        }

        # Save default mappings to file
        try:
            with open(self.team_mappings_file, 'w', encoding='utf-8') as f:
                json.dump(default_mappings, f, indent=2)
        except IOError as e:
            print(f"Warning: Could not save default team mappings: {e}")

        self._team_mappings_cache = default_mappings
        return default_mappings

    @property
    def unknown_blue_tag(self) -> str:
        return "UnknownBlue"

    @property
    def unknown_red_tag(self) -> str:
        return "UnknownRed"


@dataclass
class RosterConfig:
    """Player roster configuration"""
    team_name: str = "Gamespace MC"

    @property
    def player_ids(self) -> Dict[str, str]:
        """HLL Player IDs mapping"""
        return {
            "26433": "IceBreaker",
            "25262": "Pallet",
            "25266": "Tsiperakos",
            "20958": "Nikiyas",
            "21922": "CENTU"
        }

    @property
    def roster_riot_name_to_grid_id(self) -> Dict[str, str]:
        """Riot name to Grid ID mapping"""
        return {
            "IceBreaker": "26433",
            "Pallet": "25262",
            "Tsiperakos": "25266",
            "Nikiyas": "20958",
            "CENTU": "21922"
        }

    @property
    def player_roles_by_id(self) -> Dict[str, str]:
        """Player roles mapping"""
        return {
            "26433": "TOP",
            "25262": "JUNGLE",
            "25266": "MIDDLE",
            "20958": "BOTTOM",
            "21922": "UTILITY"
        }

    @property
    def player_display_order(self) -> list:
        """Display order for players"""
        return ["IceBreaker", "Pallet", "Tsiperakos", "Nikiyas", "CENTU"]

    @property
    def role_order_for_sheet(self) -> list:
        """Role order for data processing"""
        return ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]


@dataclass
class SoloQConfig:
    """Solo Queue tracking configuration"""

    @property
    def team_rosters(self) -> Dict[str, Dict]:
        """Solo Queue roster configuration"""
        return {
            "Gamespace": {
                "Aytekn": {
                    "game_name": ["AyteknnnN777"],
                    "tag_line": ["777"],
                    "role": "TOP"
                },
                # Add more players as needed
            }
        }

    default_region_account: str = "europe"
    default_region_match: str = "europe"
    riot_api_delay: float = 1.3


@dataclass
class AnalyticsConfig:
    """Analytics and data processing configuration"""
    # Icon sizes
    icon_size_picks_bans: int = 35
    icon_size_duos: int = 30
    icon_size_drafts: int = 24

    # Jungle pathing
    gank_presence_threshold: float = 2.0

    # Position tracking
    target_position_timestamps_sec: list = field(default_factory=lambda: [40, 60, 80])
    timestamp_tolerance_sec: float = 5.0

    # Proximity analysis
    proximity_distance_threshold: int = 2000

    # Ward analysis
    ward_vision_radius_game_units: int = 900


@dataclass
class FlaskConfig:
    """Flask application configuration"""
    secret_key: str = field(
        default_factory=lambda: os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change-in-production"))
    debug: bool = field(default_factory=lambda: os.getenv("FLASK_DEBUG", "False").lower() == "true")
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("PORT", "8080")))


class Config:
    """Main configuration class that aggregates all sub-configurations"""

    def __init__(self):
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.tournament = TournamentConfig()
        self.roster = RosterConfig()
        self.soloq = SoloQConfig()
        self.analytics = AnalyticsConfig()
        self.flask = FlaskConfig()

        # Validate configuration
        self._validate()

    def _validate(self):
        """Validate critical configuration"""
        try:
            self.api.validate()
        except ValueError as e:
            print(f"WARNING: {e}")
            print("Application will start but API features may not work.")

    def get_database_path(self) -> str:
        """Get database path (for backwards compatibility)"""
        return self.database.database_path

    def get_team_mappings(self) -> Dict[str, str]:
        """Get team mappings (for backwards compatibility)"""
        return self.tournament.team_tag_to_full_name


# Global configuration instance
config = Config()

# Export commonly used values for backwards compatibility
DATABASE_PATH = config.database.database_path
GRID_API_KEY = config.api.grid_api_key
RIOT_API_KEY = config.api.riot_api_key
GRID_BASE_URL = config.api.grid_base_url
API_REQUEST_DELAY = config.api.api_request_delay

# Tournament config
TARGET_TOURNAMENT_ID = config.tournament.tournament_id
TARGET_TOURNAMENT_NAME_FOR_DB = config.tournament.tournament_name
TEAM_TAG_TO_FULL_NAME = config.tournament.team_tag_to_full_name
UNKNOWN_BLUE_TAG = config.tournament.unknown_blue_tag
UNKNOWN_RED_TAG = config.tournament.unknown_red_tag

# Roster config
TEAM_NAME = config.roster.team_name
PLAYER_IDS = config.roster.player_ids
ROSTER_RIOT_NAME_TO_GRID_ID = config.roster.roster_riot_name_to_grid_id
PLAYER_ROLES_BY_ID = config.roster.player_roles_by_id
PLAYER_DISPLAY_ORDER = config.roster.player_display_order
ROLE_ORDER_FOR_SHEET = config.roster.role_order_for_sheet

# SoloQ config
TEAM_ROSTERS = config.soloq.team_rosters

# Analytics config
ICON_SIZE_PICKS_BANS = config.analytics.icon_size_picks_bans
ICON_SIZE_DUOS = config.analytics.icon_size_duos
ICON_SIZE_DRAFTS = config.analytics.icon_size_drafts

if __name__ == "__main__":
    # Test configuration loading
    print("=== Configuration Test ===")
    print(f"Database Path: {config.database.database_path}")
    print(f"GRID API Key Present: {bool(config.api.grid_api_key)}")
    print(f"Tournament ID: {config.tournament.tournament_id}")
    print(f"Team Mappings: {config.tournament.team_tag_to_full_name}")
    print(f"Flask Port: {config.flask.port}")
    print("=== Configuration OK ===")