# db_writer.py – DIE FINALE VERSION (funktioniert mit ALLEN Dateitypen)
from sqlalchemy.types import DateTime, Float, Text
from etl.parser_1 import RegelleistungParser
from sqlalchemy import create_engine, text
import pandas as pd

# Datenbank
engine = create_engine("sqlite:///regelleistung.db", future=True)

# Einheitliches Schema – das erlauben wir NUR!
ALLOWED_COLUMNS = {
    "delivery_date": "datetime64[ns]",
    "product": "string",
    "direction": "string",
    "awarded_capacity_mw": "float64",
    "source_file": "string"
}

def save_to_db(df: pd.DataFrame):
    if df is None or df.empty:
        print("Keine Daten zum Speichern")
        return

    # --- 1. Nur erlaubte Spalten behalten ---
    df_clean = pd.DataFrame()
    
    # delivery_date (kann "liefertag" oder direkt heißen)
    if "delivery_date" in df.columns:
        df_clean["delivery_date"] = pd.to_datetime(df["delivery_date"], dayfirst=True, errors="coerce")
    elif "liefertag" in df.columns:
        df_clean["delivery_date"] = pd.to_datetime(df["liefertag"], dayfirst=True, errors="coerce")
    else:
        df_clean["delivery_date"] = pd.NaT

    # product
    if "product" in df.columns:
        df_clean["product"] = df["product"].astype("string")
    elif "produkt" in df.columns:
        df_clean["product"] = df["produkt"].astype("string")
    else:
        df_clean["product"] = "UNKNOWN"

    # direction
    if "direction" in df.columns:
        df_clean["direction"] = df["direction"].astype("string")
    elif "richtung" in df.columns:
        df_clean["direction"] = df["richtung"].astype("string")
    else:
        df_clean["direction"] = "unknown"

    # awarded_capacity_mw – alle möglichen Spaltennamen durchprobieren
    capacity_cols = [
        "awarded_capacity_mw", "zuschlagsmenge_mw", "zuschlagskapazität_mw",
        "zuschlagskapazität_positiv_mw", "zuschlagskapazität_negativ_mw"
    ]
    capacity_data = None
    for col in capacity_cols:
        if col in df.columns:
            capacity_data = pd.to_numeric(df[col], errors="coerce")
            break
    if capacity_data is None:
        capacity_data = pd.Series([None] * len(df))
    df_clean["awarded_capacity_mw"] = capacity_data

    # source_file (für Nachverfolgung)
    source = df.attrs.get("source_file", "unknown_file.xlsx") if hasattr(df, "attrs") else "unknown"
    df_clean["source_file"] = source

    # --- 2. Nur gültige Zeilen behalten (mindestens Kapazität) ---
    df_clean = df_clean.dropna(subset=["awarded_capacity_mw"])
    df_clean = df_clean[df_clean["awarded_capacity_mw"] > 0]  # nur echte Zuschläge

    if df_clean.empty:
        print("Keine gültigen Zuschläge gefunden")
        return

    # --- 3. In DB schreiben (Schema ist jetzt immer gleich!) ---
    df_clean.to_sql(
        "capacity_awards",
        con=engine,
        if_exists="append",
        index=False,
            dtype={
            "delivery_date": DateTime(),
            "product": Text(),
            "direction": Text(),
            "awarded_capacity_mw": Float(),
            "source_file": Text(),
}
    )
    print(f"{len(df_clean)} gültige Zeilen in die Datenbank geschrieben!")


if __name__ == "__main__":
    parser = RegelleistungParser()
    df = parser.parse_latest()  # oder parse_all_files() für alle Dateien

    if df is not None and not df.empty:
        print(f"Geparst: {len(df)} Zeilen → bereinige und speichere...")
        save_to_db(df)
        
        # Gesamtstatistik
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM capacity_awards")).scalar()
            fcr = conn.execute(text("SELECT COUNT(*) FROM capacity_awards WHERE product = 'FCR'")).scalar()
        print(f"Insgesamt in DB: {total} Zeilen (davon {fcr} FCR)")
    else:
        print("Keine Daten zum Parsen")