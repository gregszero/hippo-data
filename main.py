# main.py
from pathlib import Path
from typing import List
import typer
from loader import load_pharmacies, load_events
from processor import run_pharmacy_analytics
from logger import setup_logger
import polars as pl

app = typer.Typer(help="Pharmacy Claims Analytics")

@app.command()
def main(
    pharmacy: List[Path] = typer.Option(..., "--pharmacy"),
    claims: List[Path] = typer.Option(..., "--claims"),
    reverts: List[Path] = typer.Option(..., "--reverts"),
    output_dir: Path = typer.Option(Path("output"), "--output-dir")
):
    log = setup_logger(output_dir)
    log.info("Starting pharmacy analytics pipeline")

    df_pharmacies = load_pharmacies(pharmacy)

    claims_schema = {
        "id": pl.Utf8, "npi": pl.Utf8, "ndc": pl.Utf8,
        "price": pl.Float64, "quantity": pl.Float64, "timestamp": pl.Utf8
    }
    df_claims = load_events(claims, claims_schema, "claim")

    reverts_schema = {"id": pl.Utf8, "claim_id": pl.Utf8, "timestamp": pl.Utf8}
    df_reverts = load_events(reverts, reverts_schema, "revert")

    run_pharmacy_analytics(df_claims, df_reverts, df_pharmacies, output_dir)

if __name__ == "__main__":
    app()
