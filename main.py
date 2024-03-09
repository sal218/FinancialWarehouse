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

cursor = connection.cursor()
cursor.execute("SELECT * FROM test_table")
rows = cursor.fetchall()
for row in rows:
    print(row)