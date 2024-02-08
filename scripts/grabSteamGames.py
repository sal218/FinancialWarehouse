import json
import requests
import time



# Get the list of games from the Steam API
def getSteamGames():
    success = 0
    appids = []
    url = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(url)
    data = response.json()
    for app in data['applist']['apps']:
        try:
            appids.append(app['appid'])
            if getGameDetails(app['appid']):
                    success += 1
            else:
                print("Failed: ", app['appid'])
        except:
            pass
    print("Success: ", success)
    return appids




#Get details of game based on appid
def getGameDetails(appid):
    gameDict = {}
    url = "https://store.steampowered.com/api/appdetails?appids=" + str(appid)
    response = requests.get(url)
    data = response.json()

    if not data or data[str(appid)]['success'] == False:
        return
    for key in data[str(appid)]['data']:
        try:
            gameDict[key] = data[str(appid)]['data'][key]
        except:
            pass
   
    print(gameDict['name'])
    return True

x =getSteamGames()
