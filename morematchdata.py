import json
import requests
from datetime import datetime
import time
import os


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




def main():
    try:
        print("Rufe Daten von der API ab...")
        response = requests.get('https://guest.api.arcadia.pinnacle.com/0.1/leagues/487/markets/straight')
        
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
            
            print(f"\n--- Verarbeite Match ID {match_id} ---")
            print(f"Gefundene Märkte: {len(market_data)}")
            
            # Daten in JSON-Datei speichern
            with open(f"Data/league_match_{match_id}_data.json", "w") as json_file:
                json.dump(market_data, json_file, indent=4)
            
            print(f"Daten wurden in league_match_{match_id}_data.json gespeichert.")
            
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


