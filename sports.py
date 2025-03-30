import requests
import json


def get_sports_data():
    params = {
        'brandId': '0',
    }
    response = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/sports', params=params)
    return response.json()

def main():
    # Daten abrufen
    json_data = get_sports_data()
    
    # Daten in JSON-Datei speichern (optional)
    with open('Data/sports.json', 'w') as f:
        json.dump(json_data, f, indent=4)
    

if __name__ == "__main__":
    main()