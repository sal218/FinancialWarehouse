import mysql.connector


class DatabaseManager:
    def __init__(self, host, user, database):
        self.connection = self.connect(host, user, database)

    def connect(self, host, user, database):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                database=database
            )
        except Exception as e:
            print(f"There was an error connecting to the database: {e}")
            exit()
        return connection

    def disconnect(self):
        if self.connection:
            self.connection.close()
