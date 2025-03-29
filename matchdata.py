import json
from curl_cffi import requests as cureq
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, PROXY
from datetime import datetime
import sys
import time

# Datenbank-Konfiguration
db_config = DB_CONFIG

# Liste der zu verarbeitenden IDs 
# IDs aus der Datenbank abrufen
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    # Alle parentIds aus der matches Tabelle abrufen
    cursor.execute("SELECT DISTINCT parentId FROM matches")
    parent_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    connection.close()
    
except Error as e:
    print(f"Fehler beim Abrufen der IDs aus der Datenbank: {e}")
    sys.exit(1)


def transform_market_data(market_data, target_id):
    """
    Transformiert die Marktdaten in ein einheitliches Format
    Filtert nur Einträge mit der angegebenen target_id
    """
    transformed_data = []
    
    for market in market_data:
        # Nur Einträge mit der richtigen matchupId verarbeiten
        if market.get("matchupId") != target_id:
            continue
            
        new_market = {
            "cutoffAt": market.get("cutoffAt"),
            "isAlternate": market.get("isAlternate"),
            "key": market.get("key"),
            "matchupId": market.get("matchupId"),
            "period": market.get("period"),
            "status": market.get("status"),
            "type": market.get("type"),
            "version": market.get("version")
        }
        
        # Preise und Punkte extrahieren, unabhängig vom Markttyp
        prices = market.get("prices", [])
        home_price = None
        away_price = None
        home_points = None
        away_points = None
        
        for price in prices:
            designation = price.get("designation")
            if not designation:
                continue
            
            if designation == "home":
                if "points" in price:
                    home_points = price.get("points")
                home_price = price.get("price")
            elif designation == "away":
                if "points" in price:
                    away_points = price.get("points")
                away_price = price.get("price")
            elif designation == "over" and market["type"] == "team_total":
                # Für team_total verwenden wir home für over
                if "points" in price:
                    home_points = price.get("points")
                home_price = price.get("price")
            elif designation == "under" and market["type"] == "team_total":
                # Für team_total verwenden wir away für under
                if "points" in price:
                    away_points = price.get("points")
                away_price = price.get("price")
        
        # Nur hinzufügen, wenn beide Preise vorhanden sind
        if home_price is not None and away_price is not None:
            new_market["home_price"] = home_price
            new_market["away_price"] = away_price
            
            if home_points is not None:
                new_market["home_points"] = home_points
            if away_points is not None:
                new_market["away_points"] = away_points
                
            # Falls es sich um einen team_total handelt, fügen wir die side hinzu
            if market["type"] == "team_total":
                new_market["side"] = market.get("side")
            
            transformed_data.append(new_market)
    
    return transformed_data


def save_to_database(market_data, parent_id):
    """
    Speichert die transformierten Marktdaten in der Datenbank
    Erstellt für jede parentId eine eigene Tabelle mit dem Namen match_{parentId}
    """
    if not market_data:
        print(f"Keine Daten zum Speichern für ID {parent_id} vorhanden.")
        return 0, 0
        
    # Tabellenname basierend auf parent_id erstellen
    table_name = f"match_{parent_id}"
    
    try:
        # Verbindung zur Datenbank herstellen
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Tabelle für diese parentId erstellen, falls sie nicht existiert
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                `key` VARCHAR(30) NOT NULL,
                `matchupId` BIGINT NOT NULL,
                `cutoffAt` DATETIME NOT NULL,
                `isAlternate` BOOLEAN NOT NULL,
                `period` INT NOT NULL,
                `status` VARCHAR(20) NOT NULL,
                `type` VARCHAR(20) NOT NULL,
                `version` BIGINT NOT NULL,
                `home_price` INT,
                `away_price` INT,
                `home_points` FLOAT,
                `away_points` FLOAT,
                `side` VARCHAR(10),
                
                PRIMARY KEY (`key`),
                CONSTRAINT `fk_{table_name}_matchupId` FOREIGN KEY (`matchupId`) REFERENCES `matches` (`parentId`) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            try:
                cursor.execute(create_table_query)
                print(f"Tabelle {table_name} wurde erstellt oder existierte bereits.")
            except Error as e:
                print(f"Fehler beim Erstellen der Tabelle {table_name}: {e}")
                return 0, len(market_data)
            
            # Zähle erfolgreiche und fehlgeschlagene Einfügungen
            success_count = 0
            error_count = 0
            
            # Für jeden Markteintrag
            for market in market_data:
                try:
                    # Vorbereiten der Spalten und Werte für die Anfrage
                    columns = []
                    placeholders = []
                    values = []
                    
                    for key, value in market.items():
                        columns.append(f"`{key}`")
                        placeholders.append("%s")
                        
                        # Spezielles Handling für DateTime-Strings
                        if key == "cutoffAt" and isinstance(value, str):
                            # Format: 2025-03-29T00:10:00+00:00
                            # Umwandeln in MySQL-kompatibles Format
                            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                            values.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                        else:
                            values.append(value)
                    
                    # SQL-Anfrage erstellen
                    columns_str = ", ".join(columns)
                    placeholders_str = ", ".join(placeholders)
                    
                    # Verwende REPLACE statt INSERT, um Duplikate zu behandeln (basierend auf Primärschlüssel)
                    query = f"REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"
                    
                    cursor.execute(query, values)
                    success_count += 1
                    
                except Error as e:
                    print(f"Fehler beim Einfügen des Datensatzes mit key={market.get('key')} in Tabelle {table_name}: {e}")
                    error_count += 1
            
            # Transaktion bestätigen
            connection.commit()
            print(f"Datenspeicherung in Tabelle {table_name} abgeschlossen: {success_count} erfolgreich, {error_count} fehlgeschlagen")
            
            cursor.close()
            connection.close()
            print("Datenbankverbindung geschlossen.")
            
            return success_count, error_count
            
    except Error as e:
        print(f"Fehler bei der Datenbankverbindung: {e}")
        return 0, len(market_data)
    
    finally:
        # Sicherstellen, dass die Verbindung geschlossen wird
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Datenbankverbindung geschlossen.")


def process_match_id(parent_id):
    """
    Verarbeitet eine einzelne Match-ID: Abrufen, Transformieren und Speichern der Daten
    """
    try:
        # Verwende den Proxy für die Anfrage
        proxies = PROXY
        print(f"\n--- Verarbeite Match ID {parent_id} ---")
        print(f"Rufe Daten für Match ID {parent_id} ab...")
        
        response = cureq.get(
            f'https://guest.api.arcadia.pinnacle.com/0.1/matchups/{parent_id}/markets/related/straight', 
            impersonate="chrome", 
            proxies=proxies
        )
        
        response_data = response.json()
        print(f"Originaldaten für ID {parent_id} erhalten.")
        
        # Daten transformieren und nach matchupId filtern
        transformed_data = transform_market_data(response_data, parent_id)
        print(f"Daten transformiert: {len(transformed_data)} Märkte gefunden.")
        
        if len(transformed_data) == 0:
            print(f"Keine passenden Daten für ID {parent_id} gefunden.")
            return 0, 0
        
        # In JSON-Datei speichern
        with open(f"Data/match_{parent_id}_data.json", "w") as json_file:
            json.dump(transformed_data, json_file, indent=4)
        
        print(f"Daten wurden in match_{parent_id}_data.json gespeichert.")
        
        # Daten in Datenbank speichern
        print(f"Speichere Daten für ID {parent_id} in Datenbank...")
        success, errors = save_to_database(transformed_data, parent_id)
        print(f"Datenbank-Update für ID {parent_id} abgeschlossen: {success} Einträge gespeichert, {errors} Fehler")
        
        return success, errors
        
    except Exception as e:
        print(f"Fehler bei der Verarbeitung für Match ID {parent_id}: {e}")
        return 0, 0


# Hauptprogramm
def main():
    total_success = 0
    total_errors = 0
    
    print(f"Starte Verarbeitung für {len(parent_ids)} Match-IDs...")
    
    for idx, parent_id in enumerate(parent_ids):
        print(f"\nVerarbeite ID {idx+1} von {len(parent_ids)}: {parent_id}")
        
        success, errors = process_match_id(parent_id)
        total_success += success
        total_errors += errors
        
        # Kurze Pause, um die API nicht zu überlasten
        if idx < len(parent_ids) - 1:
            print("Warte 2 Sekunden vor der nächsten Anfrage...")
            time.sleep(2)
    
    print("\n--- Verarbeitung abgeschlossen ---")
    print(f"Insgesamt wurden {total_success} Datensätze erfolgreich gespeichert.")
    print(f"Bei {total_errors} Datensätzen sind Fehler aufgetreten.")


# Programm ausführen
if __name__ == "__main__":
    main()