import oracledb
import os
from database.connection import DatabaseManager
from database.interface import DatabaseInterface
from scripts.app_details import SteamAPI
from dotenv import load_dotenv

load_dotenv()
config_dir = os.getenv('CONFIG_DIR')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
dsn = os.getenv('DSN')
wallet_location = os.getenv('WALLET_LOCATION')
wallet_password = os.getenv('WALLET_PASSWORD')

connection = oracledb.connect(
    config_dir=config_dir,
    user=user,
    password=password,
    dsn=dsn,
    wallet_location=wallet_location,
    wallet_password=wallet_password,
)

# host = "127.0.0.1"
# user = "root"
# database = "test_steam_db"

# db_manager = DatabaseManager(host, user, database)
# db_interface = DatabaseInterface(db_manager)
# steam_api = SteamAPI(db_interface)

# Assuming `connection` is your database connection object
cursor = connection.cursor()

# Execute the SELECT statement
cursor.execute("SELECT * FROM PERSONS")

# Fetch all rows from the query
rows = cursor.fetchall()

# Print all rows
for row in rows:
    print(row)
