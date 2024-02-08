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
    "name",
    "required_age",
    "is_free",
    "metacritic"
]


def insert_game(connection, game):
    cursor = connection.cursor()

    # Check if 'metacritic' key exists in game data
    metacritic_score = game['metacritic']['score'] if 'metacritic' in game and 'score' in game['metacritic'] else None

    insert_query = """
        INSERT INTO games (name, required_age, is_free, metacritic_score)
        VALUES (%s, %s, %s, %s)
    """

    cursor.execute(
        insert_query, (game['name'], game['required_age'], game['is_free'], metacritic_score))

    connection.commit()


def getSteamGames(connection):
    success = 0
    appids = []
    url = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(url)
    data = response.json()

    counter = 0
    for app in data['applist']['apps']:
        if counter >= 250:
            break
        try:
            appids.append(app['appid'])
            if getGameDetails(app['appid'], connection):
                success += 1
            else:
                print("Failed: ", app['appid'])
        except:
            pass
        counter += 1
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
    return True


def main():
    connection = connect_to_database()
    getSteamGames(connection)
    connection.close()


if __name__ == "__main__":
    main()
