
class DatabaseInterface:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def insert_steam_games(self, game):
        cursor = self.db_manager.connection.cursor()
        metacritic_score = game.get('metacritic', {}).get('score', -1)
        insert_query = """
            INSERT INTO games (steam_appid, name, required_age, is_free, metacritic_score)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query, (game['steam_appid'], game['name'], game['required_age'], game['is_free'], metacritic_score))

        self.db_manager.connection.commit()
