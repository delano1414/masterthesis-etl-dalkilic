#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.FileHandler("pipeline.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class RegelleistungParser:
    def __init__(self, speicher_ordner: str = "data"):
        self.speicher_ordner = Path(speicher_ordner)

    def parse_latest(self):
        # Nur echte Dateien, keine ~$, keine .DS_Store
        dateien = [p for p in self.speicher_ordner.iterdir()
                  if p.is_file() and "regelleistung" in p.name.lower()
                  and not p.name.startswith("~$") and not p.name.startswith(".")]
        
        if not dateien:
            logger.warning("Keine Regelleistung-Datei gefunden")
            return None

        pfad = max(dateien, key=lambda x: x.stat().st_ctime)
        logger.info(f"Parse Datei: {pfad.name}")

        if "FCR" in pfad.name and "CAPACITY" in pfad.name:
            return self._parse_fcr_capacity(pfad)
        else:
            return self._parse_standard(pfad)

    def _parse_fcr_capacity(self, pfad: Path) -> pd.DataFrame:
        # FCR Capacity ist ein wildes Excel → wir lesen alles als Text und suchen selbst
        df = pd.read_excel(pfad, header=None, engine="openpyxl")

        # 1. Lieferdatum finden
        delivery_date = pd.Timestamp("today").date()
        for val in df.values.flatten():
            val_str = str(val)
            if any(y in val_str for y in ["2024", "2025", "2026"]):
                try:
                    delivery_date = pd.to_datetime(val_str.split()[0]).date()
                    break
                except:
                    continue

        # 2. Alle numerischen Werte finden (das sind die Zuschlagskapazitäten)
        numbers = pd.to_numeric(df.stack(), errors="coerce").dropna()

        # 3. Richtung bestimmen: meistens 4 Blöcke (symmetrisch, pos 0-4, pos 4-8, neg)
        # Bei aktuellen Dateien meistens 4 Zeilen mit jeweils 4 Werten → 16 Werte
        n = len(numbers)
        if n == 0:
            return pd.DataFrame()

        # Einfachste (und zuverlässigste) Zuordnung für aktuelle Dateien:
        direction = []
        for i, val in enumerate(numbers):
            if i < n//4:
                direction.append("symmetric")
            elif i < n//2:
                direction.append("positive")
            else:
                direction.append("negative")

        result = pd.DataFrame({
            "delivery_date": [delivery_date] * len(numbers),
            "product": "FCR",
            "direction": direction,
            "awarded_capacity_mw": numbers.tolist()
        })

        result.attrs["source_file"] = pfad.name
        logger.info(f"FCR-Capacity erfolgreich geparst → {len(result)} Zeilen")
        return result

    def _parse_standard(self, pfad: Path) -> pd.DataFrame:
        if pfad.suffix.lower() == ".xlsx":
            df = pd.read_excel(pfad, skiprows=2, engine="openpyxl")
        else:
            df = pd.read_csv(pfad, sep=";", decimal=",", thousands=".", encoding="utf-8")

        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        rename_map = {
            "liefertag": "delivery_date",
            "produkt": "product",
            "richtung": "direction",
            "zuschlagsmenge_mw": "awarded_capacity_mw",
            "zuschlagskapazität_positiv_mw": "awarded_capacity_mw",
            "zuschlagskapazität_negativ_mw": "awarded_capacity_mw",
        }
        df.columns = df.columns.map(lambda c: str(c).strip())
        df.columns = df.columns.str.lower().str.replace(r"\s+", "_", regex=True)
        if "delivery_date" in df.columns:
            df["delivery_date"] = pd.to_datetime(df["delivery_date"], dayfirst=True, errors="coerce")

        df.attrs["source_file"] = pfad.name
        logger.info(f"Standard-Datei geparst → {len(df)} Zeilen")
        return df


# === TEST ===
if __name__ == "__main__":
    parser = RegelleistungParser()
    df = parser.parse_latest()
    if df is not None and len(df) > 0:
        print("\nERFOLG! Hier die geparsten Daten:")
        print(df.to_string(index=False))
    else:
        print("Leeres Ergebnis")