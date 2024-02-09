import requests
import mysql.connector


def connect_to_database():
    connection = mysql.connector.connect(
        host="127.0.0.1",  # usually localhost for XAMPP
        user="root",
        database="test_steam_db"
    )

    return connection
# Get the list of games from the Steam API


games = []
data_properties = [
    "steam_appid",
    "name",
    "required_age",
    "is_free",
    "metacritic"
]


def insert_game(connection, game):
    cursor = connection.cursor()

    # If the game doesn't exist, insert it into the database
    metacritic_score = game.get('metacritic', {}).get('score', -1)
    insert_query = """
        INSERT INTO games (steam_appid, name, required_age, is_free, metacritic_score)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(
        insert_query, (game['steam_appid'], game['name'], game['required_age'], game['is_free'], metacritic_score))

    connection.commit()


def getSteamGames(connection):
    success = 0
    appids = []
    url = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(url)
    data = response.json()

    counter = 0
    appid_counter = 10
    while True:
        if counter >= 10000:
            break
        try:
            gameObject = getGameDetails(appid_counter, connection)
            if gameObject and 'name' in gameObject:
                success += 1
                print(f"Success: {appid_counter} {gameObject['name']}")
            else:
                print("Failed: ", appid_counter)
        except:
            pass
        counter += 1
        appid_counter += 10
    print(f"{success}/{len(appids)}")
    return appids


def getGameDetails(appid, connection):
    gameDict = {}
    url = "https://store.steampowered.com/api/appdetails?appids=" + str(appid)
    response = requests.get(url)
    data = response.json()

    if not data or data[str(appid)]['success'] == False:
        return

    for key in data_properties:
        try:
            gameDict[key] = data[str(appid)]['data'][key]
        except:
            pass

    games.append(gameDict)
    insert_game(connection, gameDict)
    print(gameDict)
    return gameDict


def main():
    connection = connect_to_database()
    getSteamGames(connection)
    connection.close()


if __name__ == "__main__":
    main()
