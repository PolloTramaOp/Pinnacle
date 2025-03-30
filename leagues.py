import json
from curl_cffi import requests as cureq

BASKETBALL_SPORT_ID = 4 # 4 = Basketball
def get_leagues_data():
    params = {
        'all': 'false',
        'brandId': '0'
    }
    response = cureq.get(
        f'https://guest.api.arcadia.pinnacle.com/0.1/sports/{BASKETBALL_SPORT_ID}/leagues',
        impersonate="chrome",
        params=params
    )
    return response.json()


def main():
    # Daten abrufen
    json_data = get_leagues_data()
    
    # Daten in JSON-Datei speichern (optional)
    with open('Data/leagues.json', 'w') as f:
        json.dump(json_data, f, indent=4)
    


if __name__ == "__main__":
    main()