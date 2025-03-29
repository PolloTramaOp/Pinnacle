import json
from curl_cffi import requests as cureq
import mysql.connector
from mysql.connector import Error
from config import BASKETBALL_SPORT_ID, DB_CONFIG, PROXY

# Datenbank-Konfiguration
db_config = DB_CONFIG

def get_leagues_data():
    params = {
        'all': 'false',
        'brandId': '0'
    }
    response = cureq.get(
        f'https://guest.api.arcadia.pinnacle.com/0.1/sports/{BASKETBALL_SPORT_ID}/leagues',
        impersonate="chrome",
        params=params,
        proxies=PROXY
    )
    return response.json()

def save_to_database(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # SQL Insert Statement
        insert_query = """
        INSERT INTO leagues (
            id, ageLimit, featureOrder, `group`, isFeatured, isHidden,
            isPromoted, isSticky, matchupCount, matchupCountSE,
            name, sequence, sport_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ageLimit = VALUES(ageLimit),
            featureOrder = VALUES(featureOrder),
            `group` = VALUES(`group`),
            isFeatured = VALUES(isFeatured),
            isHidden = VALUES(isHidden),
            isPromoted = VALUES(isPromoted),
            isSticky = VALUES(isSticky),
            matchupCount = VALUES(matchupCount),
            matchupCountSE = VALUES(matchupCountSE),
            name = VALUES(name),
            sequence = VALUES(sequence),
            sport_id = VALUES(sport_id)
        """
        
        # Daten aus dem JSON extrahieren und in die Datenbank einfügen
        for league in data:
            values = (
                league.get('id', 0),
                league.get('ageLimit', 0),
                league.get('featureOrder', 0),
                league.get('group', ''),
                1 if league.get('isFeatured', False) else 0,
                1 if league.get('isHidden', False) else 0,
                1 if league.get('isPromoted', False) else 0,
                1 if league.get('isSticky', False) else 0,
                league.get('matchupCount', 0),
                league.get('matchupCountSE', 0),
                league.get('name', ''),
                league.get('sequence', 0),
                league.get('sport', {}).get('id', None)
            )
            cursor.execute(insert_query, values)
        
        connection.commit()
        print("Daten erfolgreich in die Datenbank gespeichert!")

    except Error as e:
        print(f"Fehler beim Speichern in die Datenbank: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Datenbankverbindung geschlossen.")

def main():
    # Daten abrufen
    json_data = get_leagues_data()
    
    # Daten in JSON-Datei speichern (optional)
    with open('C:\\Users\\pcost\\Desktop\\Programme\\To Do\\Pinny\\Data\\leagues.json', 'w') as f:
        json.dump(json_data, f, indent=4)
    
    # Daten in Datenbank speichern
    save_to_database(json_data)

if __name__ == "__main__":
    main()