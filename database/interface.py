from scripts.app_details import SteamGame


class DatabaseInterface:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def insert_steam_games(self, game: SteamGame):
        if not isinstance(game, SteamGame):
            raise TypeError('game must be an instance of SteamGame')

        cursor = self.db_manager.connection.cursor()
        insert_query = """
            INSERT INTO games (steam_appid, name, required_age, is_free, metacritic_score)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query, (game.steam_appid, game.name, game.required_age, game.is_free, game.metacritic))

        self.db_manager.connection.commit()
