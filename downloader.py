#!/usr/bin/env python3
import os
import requests
from datetime import datetime, timedelta
import logging
from typing import Optional

# Logging direkt ins Terminal und in eine Log-Datei
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RegelleistungDownloader:
    BASE_URL = "https://www.regelleistung.net/apps/cpp-publisher/api/v2/tenders/demands"

    def __init__(self, speicher_ordner: str = "data"):
        self.speicher_ordner = speicher_ordner
        os.makedirs(self.speicher_ordner, exist_ok=True)

    def build_url(self, tage_offset: int = -1, product_type: str = "FCR",
                  market: str = "CAPACITY", export_format: str = "xlsx") -> str:
        datum = (datetime.today() + timedelta(days=tage_offset)).strftime("%Y-%m-%d")
        params = {
            "productType": product_type,
            "market": market,
            "exportFormat": export_format,
            "deliveryDate": datum
        }
        return requests.Request("GET", self.BASE_URL, params=params).prepare().url

    def download_file(self, tage_offset: int = -1, product_type: str = "FCR",
                      market: str = "CAPACITY", export_format: str = "xlsx") -> Optional[str]:
        url = self.build_url(tage_offset, product_type, market, export_format)
        datum = (datetime.today() + timedelta(days=tage_offset)).strftime("%Y%m%d")
        ext = "csv" if export_format.lower() == "csv" else "xlsx"
        dateiname = f"{datum}_regelleistung_{product_type}_{market}.{ext}"
        pfad = os.path.join(self.speicher_ordner, dateiname)

        if os.path.exists(pfad):
            logger.info(f"Datei existiert bereits → überspringe: {dateiname}")
            return pfad

        logger.info(f"Lade herunter → {dateiname}")
        try:
            headers = {"User-Agent": "Regelleistung-Thesis/1.0 (dein@email.de)"}
            with requests.get(url, headers=headers, timeout=60, stream=True) as r:
                r.raise_for_status()
                with open(pfad, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(f"Fertig gespeichert: {pfad}")
            return pfad
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Keine Daten für {datum} {product_type} {market} verfügbar (404)")
            else:
                logger.error(f"HTTP-Fehler {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Fehler: {e}")
            return None

    def download_yesterday_all(self):
        kombis = [
            ("FCR", "CAPACITY"), ("aFRR", "CAPACITY"), ("mFRR", "CAPACITY"),
            ("FCR", "ENERGY"),   ("aFRR", "ENERGY"),   ("mFRR", "ENERGY")
        ]
        for pt, mk in kombis:
            self.download_file(tage_offset=-1, product_type=pt, market=mk)

if __name__ == "__main__":
    downloader = RegelleistungDownloader()
    print("Starte Download von gestern für alle Regelenergie-Produkte...")
    downloader.download_yesterday_all()
    print("Fertig! Dateien liegen im Ordner 'data'")