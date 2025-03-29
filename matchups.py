import json
from curl_cffi import requests as cureq
import mysql.connector
from mysql.connector import Error
from config import NBA_LEAGUE_ID, DB_CONFIG, PROXY
from datetime import datetime

# Datenbank-Konfiguration
db_config = DB_CONFIG



def get_matchups_data():
    params = {
        'brandId': '0',
    }

    # Verwende den Proxy für die Anfrage
    proxies = PROXY
    
    try:
        response = cureq.get(
            f'https://guest.api.arcadia.pinnacle.com/0.1/leagues/{NBA_LEAGUE_ID}/matchups', 
            impersonate="chrome", 
            params=params,
            proxies=proxies
        )
        return response.json()
    except Exception as e:
        print(f"Fehler bei der API-Anfrage: {e}")
        raise

def save_to_database(data):
    try:
        print("Verbinde zur Datenbank...")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        print("Datenbankverbindung hergestellt.")
        
        # SQL Insert Statement für matches
        insert_match_query = """
        INSERT INTO matches (
            isLive, league_id, parentId, home_team, away_team, startTime
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            isLive = VALUES(isLive),
            league_id = VALUES(league_id),
            startTime = VALUES(startTime)
        """
        
        print(f"Verarbeite {len(data)} Matchups...")
        
        # Prüfen, ob data ein Dictionary oder eine Liste ist
        if isinstance(data, dict):
            print("Daten sind im Dictionary-Format. Suche nach Matchups...")
            # Wenn data ein Dictionary ist, versuchen wir, die Matchup-Liste zu extrahieren
            if 'leagues' in data:
                matches = []
                for league in data['leagues']:
                    for match in league.get('matchups', []):
                        matches.append(match)
                data = matches
                print(f"Gefunden: {len(data)} Matchups in leagues")
            else:
                # Anderes bekanntes Format suchen
                for key in ['matchups', 'data', 'items']:
                    if key in data:
                        data = data[key]
                        print(f"Gefunden: {len(data)} Matchups in {key}")
                        break
        
        # Überprüfen, ob data jetzt eine Liste ist
        if not isinstance(data, list):
            print("Warnung: Unerwartetes Datenformat. Konnte keine Liste von Matchups finden.")
            print(f"Datentyp: {type(data)}")
            print(f"Dateninhalt (Ausschnitt): {str(data)[:100]} ...")
            return
        
        counter = 0
        successful_matches = 0
        
        # Daten aus dem JSON extrahieren und in die Datenbank einfügen
        for match in data:
            counter += 1
            if counter % 50 == 0:
                print(f"Verarbeite Match {counter} von {len(data)}...")
                
            if not isinstance(match, dict):
                print(f"Überspringe ungültiges Match-Objekt: {match}")
                continue
                
            try:
                # ISO-Format in MySQL-DateTime-Format umwandeln
                start_time = None
                if 'startTime' in match and match['startTime']:
                    start_time = match['startTime'].replace('T', ' ').replace('Z', '')
                
                # Prüfen ob parentId existiert
                if not match.get('parentId'):
                    print(f"Überspringe Match ohne parentId: ID {match.get('id')}")
                    continue
                
                # Extrahiere Home- und Away-Team aus dem parent-Objekt
                home_team = None
                away_team = None
                
                if 'parent' in match and match['parent'] and 'participants' in match['parent']:
                    parent_participants = match['parent'].get('participants', [])
                    for participant in parent_participants:
                        if participant.get('alignment') == 'home':
                            home_team = participant.get('name')
                        elif participant.get('alignment') == 'away':
                            away_team = participant.get('name')
                
                # Prüfen ob wir sowohl home_team als auch away_team haben
                if not home_team or not away_team:
                    print(f"Überspringe Match ohne vollständige Team-Informationen: ParentID {match.get('parentId')}")
                    continue
                
                # Match-Daten einfügen
                match_values = (
                    1 if match.get('isLive', False) else 0,
                    match.get('league', {}).get('id'),
                    match.get('parentId'),
                    home_team,
                    away_team,
                    start_time
                )
                
                try:
                    cursor.execute(insert_match_query, match_values)
                    successful_matches += 1
                except Exception as e:
                    print(f"Fehler beim Einfügen des Matches: {e}")
                    print(f"ParentID: {match.get('parentId')}, Home: {home_team}, Away: {away_team}")
                    continue
                
                # Nach jedem 100. Eintrag committen
                if counter % 100 == 0:
                    connection.commit()
                    print(f"Zwischenstand: {counter} Einträge verarbeitet, {successful_matches} erfolgreich gespeichert.")
                    
            except Exception as e:
                print(f"Fehler bei Match {counter}: {e}")
                # Fehler nicht weitergeben, sondern mit nächstem Match fortfahren
                continue
        
        # Abschließendes Commit
        print("Führe finales Commit durch...")
        connection.commit()
        print(f"Daten erfolgreich in die Datenbank gespeichert!")
        print(f"{counter} Einträge verarbeitet, {successful_matches} erfolgreich gespeichert.")

    except Error as e:
        print(f"Fehler beim Speichern in die Datenbank: {e}")
        # Stacktrace ausgeben
        import traceback
        traceback.print_exc()
    
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Datenbankverbindung geschlossen.")

def main():
    # Daten abrufen
    print("Rufe Matchup-Daten von der API ab...")
    try:
        json_data = get_matchups_data()
        print("Daten erfolgreich abgerufen.")
        
        # Daten in JSON-Datei speichern (optional)
        print("Speichere Daten in JSON-Datei...")
        with open('C:\\Users\\pcost\\Desktop\\Programme\\To Do\\Pinny\\Data\\matchups.json', 'w') as f:
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
        
        # Daten in Datenbank speichern
        print("Beginne mit dem Speichern der Daten in der Datenbank...")
        save_to_database(json_data)
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")
        import traceback
        traceback.print_exc()
    
    print("Programm beendet.")

if __name__ == "__main__":
    main()