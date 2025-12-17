# Regelleistung ETL Pipeline (Master Thesis)

This repository contains an **ETL pipeline for German balancing power market data (Regelleistung)**.
The pipeline downloads auction results from *regelleistung.net*, parses heterogeneous raw files and
stores normalized results in a relational database.

Pipeline orchestration and dependency management are implemented using **Dagster**.

---

## Architecture Overview

The pipeline is structured into modular ETL components:

- **downloader.py**
  - Downloads auction result files (xlsx / csv) from the regelleistung.net API
  - Supports multiple product types (FCR, aFRR, mFRR) and markets (capacity, energy)

- **parser_1.py**
  - Parses raw input files
  - Handles inconsistent file structures (especially FCR capacity Excel files)
  - Normalizes columns and data types

- **db_writer.py**
  - Cleans parsed data
  - Writes validated records into a SQLite database using SQLAlchemy

- **dagster_app/**
  - `assets.py`: Dagster asset definitions wrapping ETL steps
  - `definitions.py`: Dagster repository and asset registration

---

## Technology Stack

- **Python 3.11**
- **Dagster** – pipeline orchestration
- **Pandas** – data processing
- **SQLAlchemy** – database abstraction
- **SQLite** – lightweight relational database

---

## Data Flow

1. **Raw Files**
   - Downloaded from regelleistung.net
2. **Parsed Tables**
   - Normalized Pandas DataFrames
3. **Database Load**
   - Validated data written to SQLite (`capacity_awards` table)

Dagster manages dependencies and execution order between these steps.

---

## How to Run

### 1. Install dependencies
pip install -r requirements.txt

### 2. Start Dagster
PYTHONPATH=$(pwd) dagster dev -m dagster_app.definitions

### Open Dagster UI
http://127.0.0.1:3000

From the UI, individual assets or the full pipeline can be materialized.

---

### Projektstruktur
Masterthesis/
│
├── dagster_app/
│   ├── __init__.py            # Dagster-Modul
│   ├── assets.py              # Definition der Dagster Assets (ETL-Orchestrierung)
│   └── definitions.py         # Zentrale Dagster-Definitionen
│
├── etl/
│   ├── downloader.py          # Download der Regelleistungsdaten
│   ├── parser_1.py            # Parsing und Aufbereitung der Rohdaten
│   └── db_writer.py           # Speicherung der Daten in SQLite (SQLAlchemy)
│
├── requirements.txt           # Python-Abhängigkeiten
└── README.md                  # Projektbeschreibung

