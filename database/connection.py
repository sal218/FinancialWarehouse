import mysql.connector


class DatabaseManager:
    def __init__(self, host, user, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            database=database
        )
