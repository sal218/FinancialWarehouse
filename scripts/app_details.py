import requests
from tqdm import tqdm


class SteamGame:
    def __init__(self, steam_appid: int, name: str, required_age: int, is_free: int, metacritic: int):
        self.steam_appid = steam_appid
        self.name = name
        self.required_age = required_age
        self.is_free = is_free
        self.metacritic = metacritic

    data_properties = [
        "steam_appid",
        "name",
        "required_age",
        "is_free",
        "metacritic"
    ]


class SteamAPI:
    def __init__(self, db_interface):
        self.db_interface = db_interface

    def get_steam_games(self):
        appid_counter = 50000
        appid_end = 100000
        with tqdm(total=appid_end-appid_counter, colour='green') as pbar:
            pbar.set_description("Processing app ids")
            for i in range(appid_counter, appid_end, 10):
                try:
                    game_object = self.get_game_details(i)
                    if game_object:
                        game = SteamGame(game_object['steam_appid'], game_object['name'], game_object['required_age'],
                                         game_object['is_free'], game_object.get('metacritic', {}).get('score', -1))
                        self.db_interface.insert_steam_games(game)
                    pbar.update(10)

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
