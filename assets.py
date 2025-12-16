from  __future__ import annotations

from dagster import asset, AssetExecutionContext, MetadataValue
from pathlib import Path
import pandas as pd

from etl.downloader import RegelleistungDownloader
from etl.parser_1 import RegelleistungParser
from etl.db_writer import save_to_db

DEFAULT_COMBOS = [
    ("FCR", "CAPACITY"),
    ("aFRR", "CAPACITY"),
    ("mFRR", "CAPACITY"),
    ("FCR", "ENERGY"),
    ("aFRR", "ENERGY"),
    ("mFRR", "ENERGY"),
]


def parse_file(parser: RegelleistungParser, pfad: Path) -> pd.DataFrame:
    name = pfad.name
    if "FCR" in name and "CAPACITY" in name:
        df = parser._parse_fcr_capacity(pfad)
    else:
        df = parser._parse_standard(pfad)
    df.attrs["source_file"] = name
    return df


@asset
def raw_files(context) -> list[str]:
    data_dir = Path("data")
    downloader = RegelleistungDownloader(speicher_ordner=str(data_dir))

    saved, missing = [], []
    for product, market in DEFAULT_COMBOS:
        pfad = downloader.download_file(
            tage_offset=-1, product_type=product, market=market, export_format="xlsx"
        )
        if pfad:
            saved.append(pfad)
        else:
            missing.append(f"{product}-{market}")

    context.add_output_metadata({
        "saved_count": len(saved),
        "missing_count": len(missing),
        "missing": MetadataValue.json(missing),
        "paths_preview": MetadataValue.json(saved[:5]),
    })
    return saved


@asset
def parsed_tables(context, raw_files: list[str]) -> list[dict]:
    parser = RegelleistungParser(speicher_ordner="data")
    out = []

    for p in raw_files:
        pfad = Path(p)
        if not pfad.exists():
            continue
        df = parse_file(parser, pfad)
        out.append({"source_file": pfad.name, "rows": int(len(df)), "df": df})

    context.add_output_metadata({
        "files_parsed": len(out),
        "total_rows": sum(x["rows"] for x in out),
        "sources": MetadataValue.json([x["source_file"] for x in out]),
    })
    return out


@asset
def db_load(context, parsed_tables: list[dict]) -> dict:
    parsed_rows_by_file = {}
    for item in parsed_tables:
        df: pd.DataFrame = item["df"]
        save_to_db(df)
        parsed_rows_by_file[item["source_file"]] = int(len(df))

    context.add_output_metadata({
        "parsed_rows_by_file": MetadataValue.json(parsed_rows_by_file),
    })
    return {"parsed_rows_by_file": parsed_rows_by_file}
