import requests
import json
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, PROXY


def get_sports_data():
    params = {
        'brandId': '0',
    }
    response = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/sports', params=params, proxies=PROXY)
    return response.json()

def save_to_database(data):
    try:
        db_config = DB_CONFIG
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # SQL Insert Statement
        insert_query = """
        INSERT INTO sport (
            id, featureOrder, isFeatured, isHidden, isSticky,
            matchupCount, matchupCountSE, name, primaryMarketType
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            featureOrder = VALUES(featureOrder),
            isFeatured = VALUES(isFeatured),
            isHidden = VALUES(isHidden),
            isSticky = VALUES(isSticky),
            matchupCount = VALUES(matchupCount),
            matchupCountSE = VALUES(matchupCountSE),
            name = VALUES(name),
            primaryMarketType = VALUES(primaryMarketType)
        """
        
        # Daten aus dem JSON extrahieren und in die Datenbank einfügen
        for sport in data:
            values = (
                sport.get('id', 0),
                sport.get('featureOrder', 0),
                sport.get('isFeatured', 0),
                sport.get('isHidden', 0),
                sport.get('isSticky', 0),
                sport.get('matchupCount', 0),
                sport.get('matchupCountSE', 0),
                sport.get('name', ''),
                sport.get('primaryMarketType', '')
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
    json_data = get_sports_data()
    
    # Daten in JSON-Datei speichern (optional)
    with open('C:\\Users\\pcost\\Desktop\\Programme\\To Do\\Pinny\\Data\\sports.json', 'w') as f:
        json.dump(json_data, f, indent=4)
    
    # Daten in Datenbank speichern
    save_to_database(json_data)

if __name__ == "__main__":
    main()