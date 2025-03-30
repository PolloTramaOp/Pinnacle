import json
from curl_cffi import requests as cureq
from datetime import datetime
import sys
import time




parent_ids = [1606491376] #NBA EXAMPLE



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


def process_match_id(parent_id):
    """
    Verarbeitet eine einzelne Match-ID: Abrufen, Transformieren und Speichern der Daten
    """
    try:

        print(f"\n--- Verarbeite Match ID {parent_id} ---")
        print(f"Rufe Daten für Match ID {parent_id} ab...")
        
        response = cureq.get(
            f'https://guest.api.arcadia.pinnacle.com/0.1/matchups/{parent_id}/markets/related/straight', 
            impersonate="chrome"
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

    except Exception as e:
        print(f"Fehler bei der Verarbeitung für Match ID {parent_id}: {e}")
        return 0, 0


# Hauptprogramm
def main():

    
    print(f"Starte Verarbeitung für {len(parent_ids)} Match-IDs...")
    
    for idx, parent_id in enumerate(parent_ids):
        print(f"\nVerarbeite ID {idx+1} von {len(parent_ids)}: {parent_id}")
        
        process_match_id(parent_id)

        
        # Kurze Pause, um die API nicht zu überlasten
        if idx < len(parent_ids) - 1:
            print("Warte 2 Sekunden vor der nächsten Anfrage...")
            time.sleep(2)


# Programm ausführen
if __name__ == "__main__":
    main()