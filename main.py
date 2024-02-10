from database.connection import DatabaseManager
from database.interface import DatabaseInterface
from scripts.app_details import SteamAPI

host = "127.0.0.1"
user = "root"
database = "test_steam_db"

db_manager = DatabaseManager(host, user, database)
db_interface = DatabaseInterface(db_manager)
steam_api = SteamAPI(db_manager, db_interface)

steam_api.get_steam_games()
