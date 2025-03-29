import json
import requests
import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG, PROXY
from datetime import datetime
import sys
import time
import os

# Datenbank-Konfiguration
db_config = DB_CONFIG

# Erstellen Sie den Data-Ordner, falls er nicht existiert
os.makedirs("Data", exist_ok=True)


def transform_market_data(market_data):
    """
    Transformiert die Marktdaten in ein einheitliches Format
    Gruppiert die Daten nach matchupId
    """
    transformed_data_by_matchup = {}
    
    for market in market_data:
        matchup_id = market.get("matchupId")
        if not matchup_id:
            continue
            
        new_market = {
            "cutoffAt": market.get("cutoffAt"),
            "isAlternate": market.get("isAlternate"),
            "key": market.get("key"),
            "matchupId": matchup_id,
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
            
            # Nach matchupId gruppieren
            if matchup_id not in transformed_data_by_matchup:
                transformed_data_by_matchup[matchup_id] = []
            
            transformed_data_by_matchup[matchup_id].append(new_market)
    
    return transformed_data_by_matchup


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


def check_match_exists(match_id):
    """
    Überprüft, ob ein Match mit der angegebenen ID in der matches-Tabelle existiert
    """
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM matches WHERE parentId = %s", (match_id,))
        count = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        return count > 0
        
    except Error as e:
        print(f"Fehler beim Überprüfen der Match-ID {match_id}: {e}")
        return False


def main():
    try:
        print("Rufe Daten von der API ab...")
        response = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/leagues/487/markets/straight', proxies=PROXY)
        
        if response.status_code != 200:
            print(f"Fehler beim Abrufen der Daten: HTTP-Statuscode {response.status_code}")
            return
        
        response_data = response.json()
        print(f"Daten erfolgreich abgerufen: {len(response_data)} Einträge gefunden.")
        
        # Gruppieren der Daten nach matchupId
        transformed_data_by_matchup = transform_market_data(response_data)
        print(f"Daten nach matchupId gruppiert: {len(transformed_data_by_matchup)} verschiedene Matches gefunden.")
        
        # Gesamtstatistiken
        total_success = 0
        total_errors = 0
        processed_matches = 0
        skipped_matches = 0
        
        # Jede matchupId einzeln verarbeiten
        for match_id, market_data in transformed_data_by_matchup.items():
            # Überprüfen, ob dieses Match bereits in der Datenbank existiert
            if not check_match_exists(match_id):
                print(f"Match ID {match_id} ist nicht in der Datenbank vorhanden und wird übersprungen.")
                skipped_matches += 1
                continue
            
            print(f"\n--- Verarbeite Match ID {match_id} ---")
            print(f"Gefundene Märkte: {len(market_data)}")
            
            # Daten in JSON-Datei speichern
            with open(f"Data/league_match_{match_id}_data.json", "w") as json_file:
                json.dump(market_data, json_file, indent=4)
            
            print(f"Daten wurden in league_match_{match_id}_data.json gespeichert.")
            
            # Daten in Datenbank speichern
            print(f"Speichere Daten für Match ID {match_id} in Datenbank...")
            success, errors = save_to_database(market_data, match_id)
            print(f"Datenbank-Update für Match ID {match_id} abgeschlossen: {success} Einträge gespeichert, {errors} Fehler")
            
            total_success += success
            total_errors += errors
            processed_matches += 1
            
            # Kurze Pause zwischen den Datenbankoperationen
            if processed_matches < len(transformed_data_by_matchup):
                time.sleep(1)
        
        print("\n--- Verarbeitung abgeschlossen ---")
        print(f"Insgesamt wurden {processed_matches} Matches verarbeitet und {skipped_matches} übersprungen.")
        print(f"{total_success} Datensätze wurden erfolgreich gespeichert, bei {total_errors} traten Fehler auf.")
        
    except Exception as e:
        print(f"Fehler bei der Verarbeitung: {e}")


if __name__ == "__main__":
    main()


