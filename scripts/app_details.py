import requests


class SteamGame:
    def __init__(self, appid, name, required_age, is_free, metacritic_score):
        self.appid = appid
        self.name = name
        self.required_age = required_age
        self.is_free = is_free
        self.metacritic_score = metacritic_score

    data_properties = [
        "steam_appid",
        "name",
        "required_age",
        "is_free",
        "metacritic"
    ]


class SteamAPI:
    def __init__(self, db_manager, db_interface):
        self.db_manager = db_manager
        self.db_interface = db_interface

    def get_steam_games(self):
        appid_counter = 10
        appid_end = 100
        for i in range(appid_counter, appid_end, 10):
            try:
                game_object = self.get_game_details(i)
                if game_object:
                    self.db_interface.insert_steam_games(game_object)
                # if game_object and 'name' in game_object:
                #     game = SteamGame(game_object['steam_appid'], game_object['name'], game_object['required_age'],
                #                      game_object['is_free'], game_object.get('metacritic', {}).get('score', -1))
                #     self.db_manager.insert_steam_games(game)

            except Exception as e:
                print(f"Error: {e}")

    def get_game_details(self, appid):
        gameDict = {}
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        response = requests.get(url)
        data = response.json()

        if not data or data[str(appid)]['success'] == False:
            return

        for key in SteamGame.data_properties:
            try:
                gameDict[key] = data[str(appid)]['data'][key]
            except:
                pass

        return gameDict
