import json
from curl_cffi import requests as cureq
from datetime import datetime


LEAGUE_ID = 487 #NBA EXAMPLE

def get_matchups_data():
    params = {
        'brandId': '0',
    }

    
    try:
        response = cureq.get(
            f'https://guest.api.arcadia.pinnacle.com/0.1/leagues/{LEAGUE_ID}/matchups', 
            impersonate="chrome", 
            params=params
        )
        return response.json()
    except Exception as e:
        print(f"Fehler bei der API-Anfrage: {e}")
        raise

def main():
    # Daten abrufen
    print("Rufe Matchup-Daten von der API ab...")
    try:
        json_data = get_matchups_data()
        print("Daten erfolgreich abgerufen.")
        
        # Daten in JSON-Datei speichern (optional)
        print("Speichere Daten in JSON-Datei...")
        with open('Data/matchups.json', 'w') as f:
            json.dump(json_data, f, indent=4)
        print("JSON-Datei gespeichert.")
        
        # Debug-Ausgabe zum Verständnis der Datenstruktur
        print(f"Datentyp: {type(json_data)}")
        if isinstance(json_data, list):
            print(f"Anzahl der Elemente: {len(json_data)}")
            if len(json_data) > 0:
                print(f"Typ des ersten Elements: {type(json_data[0])}")
        elif isinstance(json_data, dict):
            print(f"Schlüssel im Dictionary: {json_data.keys()}")
        
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")
        import traceback
        traceback.print_exc()
    
    print("Programm beendet.")

if __name__ == "__main__":
    main()