# lol_app_LTA_2/database.py
# lol_app_LTA/database.py

import sqlite3
import os
import sys
from datetime import datetime, timezone

# Import configuration
try:
    from config import config, DATABASE_PATH
except ImportError:
    # Fallback if config.py is not available
    _basedir = os.path.abspath(os.path.dirname(__file__))
    # FIXED: Correct path joining without leading slash
    DATABASE_PATH = os.path.join(_basedir, 'data', 'scrims_data.db')
    print(f"Warning: Using fallback database path: {DATABASE_PATH}")

# --- Заголовки таблиц ---
SCRIMS_HEADER = [
    "Date", "Patch", "Blue Team Name", "Red Team Name", "Duration", "Result",
    "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    "Blue_TOP_Player", "Blue_TOP_Champ", "Blue_TOP_K", "Blue_TOP_D", "Blue_TOP_A", "Blue_TOP_Dmg", "Blue_TOP_CS",
    "Blue_JGL_Player", "Blue_JGL_Champ", "Blue_JGL_K", "Blue_JGL_D", "Blue_JGL_A", "Blue_JGL_Dmg", "Blue_JGL_CS",
    "Blue_MID_Player", "Blue_MID_Champ", "Blue_MID_K", "Blue_MID_D", "Blue_MID_A", "Blue_MID_Dmg", "Blue_MID_CS",
    "Blue_BOT_Player", "Blue_BOT_Champ", "Blue_BOT_K", "Blue_BOT_D", "Blue_BOT_A", "Blue_BOT_Dmg", "Blue_BOT_CS",
    "Blue_SUP_Player", "Blue_SUP_Champ", "Blue_SUP_K", "Blue_SUP_D", "Blue_SUP_A", "Blue_SUP_Dmg", "Blue_SUP_CS",
    "Red_TOP_Player", "Red_TOP_Champ", "Red_TOP_K", "Red_TOP_D", "Red_TOP_A", "Red_TOP_Dmg", "Red_TOP_CS",
    "Red_JGL_Player", "Red_JGL_Champ", "Red_JGL_K", "Red_JGL_D", "Red_JGL_A", "Red_JGL_Dmg", "Red_JGL_CS",
    "Red_MID_Player", "Red_MID_Champ", "Red_MID_K", "Red_MID_D", "Red_MID_A", "Red_MID_Dmg", "Red_MID_CS",
    "Red_BOT_Player", "Red_BOT_Champ", "Red_BOT_K", "Red_BOT_D", "Red_BOT_A", "Red_BOT_Dmg", "Red_BOT_CS",
    "Red_SUP_Player", "Red_SUP_Champ", "Red_SUP_K", "Red_SUP_D", "Red_SUP_A", "Red_SUP_Dmg", "Red_SUP_CS",
    "Game ID"
]

TOURNAMENT_GAMES_HEADER_BASE = [
    "Tournament Name", "Stage Name", "Date", "Patch", "Blue Team Name", "Red Team Name",
    "Duration", "Winner Side", "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    "Blue_TOP_Champ", "Blue_JGL_Champ", "Blue_MID_Champ", "Blue_BOT_Champ", "Blue_SUP_Champ",
    "Red_TOP_Champ", "Red_JGL_Champ", "Red_MID_Champ", "Red_BOT_Champ", "Red_SUP_Champ",
    "Blue_TOP_PUUID", "Blue_JGL_PUUID", "Blue_MID_PUUID", "Blue_BOT_PUUID", "Blue_SUP_PUUID",
    "Red_TOP_PUUID", "Red_JGL_PUUID", "Red_MID_PUUID", "Red_BOT_PUUID", "Red_SUP_PUUID",
    "Blue_TOP_PartID", "Blue_JGL_PartID", "Blue_MID_PartID", "Blue_BOT_PartID", "Blue_SUP_PartID",
    "Red_TOP_PartID", "Red_JGL_PartID", "Red_MID_PartID", "Red_BOT_PartID", "Red_SUP_PartID",
    "Game ID", "Series ID", "Sequence Number"
]
draft_action_columns = []
for i in range(1, 21):
    draft_action_columns.extend([f"Draft_Action_{i}_Type", f"Draft_Action_{i}_TeamID", f"Draft_Action_{i}_ChampName",
                                 f"Draft_Action_{i}_ChampID", f"Draft_Action_{i}_ActionID"])
TOURNAMENT_GAMES_HEADER = TOURNAMENT_GAMES_HEADER_BASE + draft_action_columns

SOLOQ_GAMES_HEADER = [
    "Match_ID", "Player_Name", "Riot_Name", "Riot_Tag", "Timestamp", "Date_Readable",
    "Win", "Champion", "Role", "Kills", "Deaths", "Assists"
]

manual_draft_action_headers = [f"action_{i}_champion" for i in range(1, 21)]
MANUAL_DRAFTS_HEADER = [
                           "id", "filter_team_name", "game_index", "blue_team_editable_name",
                           "red_team_editable_name",
                       ] + manual_draft_action_headers + ["last_updated"]


def get_db_connection():
    """Creates and returns a connection to the SQLite database with improved error handling."""
    conn = None
    try:
        # Ensure the directory exists
        db_dir = os.path.dirname(DATABASE_PATH)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"Created database directory: {db_dir}")

        conn = sqlite3.connect(DATABASE_PATH, timeout=10.0)
        conn.row_factory = sqlite3.Row
        print(f"Database connection established: {DATABASE_PATH}")
    except sqlite3.Error as e:
        print(f"ERROR: SQLite connection failed: {e}")
        print(f"Attempted path: {DATABASE_PATH}")
    except Exception as e:
        print(f"ERROR: Unexpected error during database connection: {e}")
    return conn


def create_table_from_header(cursor, table_name, header_list, primary_key_column="Game ID"):
    """Helper function to create a table from header list with validation."""
    # Security: Validate table name against whitelist
    ALLOWED_TABLES = {
        'scrims', 'tournament_games', 'soloq_games', 'manual_drafts',
        'schedule_entries', 'schedule_notes', 'jungle_pathing',
        'player_positions_snapshots', 'first_wards_data', 'all_wards_data',
        'player_positions_timeline', 'objective_events'
    }

    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}. Must be one of {ALLOWED_TABLES}")

    columns_sql = []
    header_copy = list(header_list)
    pk_col_name_sql = primary_key_column.replace(" ", "_").replace(".", "").replace("-", "_")

    pk_col_type = "INTEGER" if primary_key_column == "id" else "TEXT"
    pk_sql = f'"{pk_col_name_sql}" {pk_col_type} PRIMARY KEY'
    if pk_col_type == "INTEGER":
        pk_sql += " AUTOINCREMENT"
    columns_sql.append(pk_sql)

    try:
        header_copy.remove(primary_key_column)
    except ValueError:
        try:
            header_copy.remove(pk_col_name_sql)
        except ValueError:
            print(
                f"Warning: Primary key '{primary_key_column}' or '{pk_col_name_sql}' not found in header for table '{table_name}'.")

    for header_name in header_copy:
        col_name_sql = header_name.replace(" ", "_").replace(".", "").replace("-", "_")
        col_type = "TEXT"

        is_scrims_numeric = table_name == "scrims" and \
                            any(x in header_name for x in ["_K", "_D", "_A", "_Dmg", "_CS"])
        is_soloq_numeric = table_name == "soloq_games" and \
                           header_name in ["Win", "Timestamp", "Kills", "Deaths", "Assists"]
        is_tourn_numeric = table_name == "tournament_games" and \
                           (header_name == "Sequence Number" or "PartID" in header_name)
        is_manual_numeric = table_name == "manual_drafts" and \
                            header_name == "game_index"

        if is_scrims_numeric or is_soloq_numeric or is_tourn_numeric or is_manual_numeric:
            col_type = "INTEGER DEFAULT 0"
            if header_name == "game_index" and table_name == "manual_drafts":
                col_type = "INTEGER DEFAULT 1"

        if table_name == "manual_drafts" and col_name_sql in ["filter_team_name", "game_index"]:
            col_type += " NOT NULL"

        columns_sql.append(f'"{col_name_sql}" {col_type}')

    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_sql)});'
    try:
        cursor.execute(create_table_sql)
        print(f"Table '{table_name}' verified/created successfully.")
        return True
    except sqlite3.Error as e:
        print(f"ERROR creating table '{table_name}': {e}")
        return False


def init_db():
    """Initializes the database: creates tables if they don't exist."""
    conn = get_db_connection()
    if conn is None:
        print("ERROR: Could not connect to database for initialization.")
        return

    cursor = conn.cursor()
    try:
        # --- Existing tables ---
        print("Checking/creating table scrims...")
        create_table_from_header(cursor, "scrims", SCRIMS_HEADER, primary_key_column="Game ID")

        print("Checking/creating table tournament_games...")
        create_table_from_header(cursor, "tournament_games", TOURNAMENT_GAMES_HEADER, primary_key_column="Game ID")

        print("Checking/creating table soloq_games...")
        create_table_from_header(cursor, "soloq_games", SOLOQ_GAMES_HEADER, primary_key_column="Match_ID")

        print("Checking/creating table manual_drafts...")
        if create_table_from_header(cursor, "manual_drafts", MANUAL_DRAFTS_HEADER, primary_key_column="id"):
            try:
                cursor.execute(
                    'CREATE UNIQUE INDEX IF NOT EXISTS idx_manual_drafts_team_game ON manual_drafts (filter_team_name, game_index);')
                print("Unique index for 'manual_drafts' (team, game_index) verified/created.")
            except sqlite3.Error as e:
                print(f"ERROR creating unique index for 'manual_drafts': {e}")

        print("Checking/creating table schedule_entries...")
        create_schedule_sql = "CREATE TABLE IF NOT EXISTS schedule_entries (id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT NOT NULL, entry_type TEXT NOT NULL, details_time TEXT, details_opponent TEXT, details_notes TEXT, color TEXT);"
        cursor.execute(create_schedule_sql)
        print("Table 'schedule_entries' verified/created.")

        print("Checking/creating table schedule_notes...")
        create_notes_sql = "CREATE TABLE IF NOT EXISTS schedule_notes (month_id TEXT PRIMARY KEY, notes_content TEXT DEFAULT '');"
        cursor.execute(create_notes_sql)
        print("Table 'schedule_notes' (monthly) verified/created.")

        print("Checking/creating table jungle_pathing...")
        create_pathing_sql = """
        CREATE TABLE IF NOT EXISTS jungle_pathing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            path_sequence TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );"""
        create_pathing_index_sql = "CREATE INDEX IF NOT EXISTS idx_jungle_pathing_game_id ON jungle_pathing (game_id);"
        create_pathing_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_jungle_pathing_game_player ON jungle_pathing (game_id, player_puuid);"
        try:
            cursor.execute(create_pathing_sql)
            cursor.execute(create_pathing_index_sql)
            cursor.execute(create_pathing_unique_sql)
            print("Table 'jungle_pathing' and indexes verified/created.")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'jungle_pathing': {e}")

        print("Checking/creating table player_positions_snapshots...")
        create_positions_sql = """
        CREATE TABLE IF NOT EXISTS player_positions_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_seconds INTEGER NOT NULL,
            positions_json TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );"""
        create_positions_index_sql = "CREATE INDEX IF NOT EXISTS idx_player_positions_game_id ON player_positions_snapshots (game_id);"
        create_positions_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_player_positions_game_timestamp ON player_positions_snapshots (game_id, timestamp_seconds);"
        try:
            cursor.execute(create_positions_sql)
            cursor.execute(create_positions_index_sql)
            cursor.execute(create_positions_unique_sql)
            print("Table 'player_positions_snapshots' and indexes verified/created.")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'player_positions_snapshots': {e}")

        print("Checking/creating table first_wards_data...")
        create_first_wards_sql = """
        CREATE TABLE IF NOT EXISTS first_wards_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            participant_id INTEGER,
            player_name TEXT,
            champion_name TEXT,
            ward_type TEXT,
            timestamp_seconds REAL NOT NULL,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );"""
        create_first_wards_unique_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_first_wards_game_player ON first_wards_data (game_id, player_puuid);"
        create_first_wards_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_first_wards_data_game_id ON first_wards_data (game_id);"
        try:
            cursor.execute(create_first_wards_sql)
            cursor.execute(create_first_wards_unique_sql)
            cursor.execute(create_first_wards_game_id_index_sql)
            print("Table 'first_wards_data' and indexes verified/created (with player_name column).")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'first_wards_data': {e}")

        print("Checking/creating table all_wards_data...")
        create_all_wards_sql = """
        CREATE TABLE IF NOT EXISTS all_wards_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_puuid TEXT NOT NULL,
            participant_id INTEGER,
            player_name TEXT,
            champion_name TEXT,
            ward_type TEXT,
            timestamp_seconds REAL NOT NULL,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );
        """
        create_all_wards_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_game_id ON all_wards_data (game_id);"
        create_all_wards_puuid_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_player_puuid ON all_wards_data (player_puuid);"
        create_all_wards_timestamp_index_sql = "CREATE INDEX IF NOT EXISTS idx_all_wards_data_timestamp ON all_wards_data (timestamp_seconds);"
        try:
            cursor.execute(create_all_wards_sql)
            cursor.execute(create_all_wards_game_id_index_sql)
            cursor.execute(create_all_wards_puuid_index_sql)
            cursor.execute(create_all_wards_timestamp_index_sql)
            print("Table 'all_wards_data' and indexes verified/created.")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'all_wards_data': {e}")

        print("Checking/creating table player_positions_timeline...")
        create_positions_timeline_sql = """
        CREATE TABLE IF NOT EXISTS player_positions_timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            participant_id INTEGER NOT NULL,
            player_puuid TEXT,
            pos_x INTEGER,
            pos_z INTEGER,
            last_updated TEXT NOT NULL
        );
        """
        create_timeline_game_id_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_game_id ON player_positions_timeline (game_id);"
        create_timeline_timestamp_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_timestamp ON player_positions_timeline (timestamp_ms);"
        create_timeline_game_puuid_index_sql = "CREATE INDEX IF NOT EXISTS idx_timeline_game_puuid ON player_positions_timeline (game_id, player_puuid);"
        try:
            cursor.execute(create_positions_timeline_sql)
            cursor.execute(create_timeline_game_id_index_sql)
            cursor.execute(create_timeline_timestamp_index_sql)
            cursor.execute(create_timeline_game_puuid_index_sql)
            print("Table 'player_positions_timeline' and indexes verified/created.")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'player_positions_timeline': {e}")

        print("Checking/creating table objective_events...")
        create_objectives_sql = """
        CREATE TABLE IF NOT EXISTS objective_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            objective_type TEXT NOT NULL,
            objective_subtype TEXT,
            team_id INTEGER,
            killer_participant_id INTEGER,
            lane TEXT
        );
        """
        create_objectives_game_id_index = "CREATE INDEX IF NOT EXISTS idx_objectives_game_id ON objective_events (game_id);"
        create_objectives_type_index = "CREATE INDEX IF NOT EXISTS idx_objectives_type ON objective_events (objective_type);"
        try:
            cursor.execute(create_objectives_sql)
            cursor.execute(create_objectives_game_id_index)
            cursor.execute(create_objectives_type_index)
            print("Table 'objective_events' and indexes verified/created.")
        except sqlite3.Error as e:
            print(f"ERROR creating table/indexes 'objective_events': {e}")

        conn.commit()
        print("Database initialization completed successfully.")
    except sqlite3.Error as e:
        print(f"ERROR during database initialization: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    print(f"!!! Database path: {DATABASE_PATH}")
    print(f"!!! NOTICE: Schema includes tournament_games with PUUID/PartID columns.")
    print(f"!!! NOTICE: New tables for jungle_pathing, player_positions_snapshots, wards, and objectives.")
    print(f"!!! If errors occur after update, consider deleting old database file and restarting.")
    init_db()
    print("Database initialization script completed.")