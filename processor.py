import polars as pl
import json
from pathlib import Path
from logger import setup_logger

log = setup_logger()


def filter_to_pharmacy_dataset(
    df_claims: pl.DataFrame,
    df_pharmacies: pl.DataFrame
) -> pl.DataFrame:
    """Filter claims to only those with NPI in the pharmacy dataset."""
    valid_npis = df_pharmacies.select("npi")
    before = len(df_claims)
    df_claims = df_claims.join(valid_npis, left_on="npi", right_on="npi", how="inner")
    dropped = before - len(df_claims)
    if dropped:
        log.info(f"Dropped {dropped} claims with unknown NPI")
    return df_claims


def mark_reverted_claims(
    df_claims: pl.DataFrame,
    df_reverts: pl.DataFrame
) -> pl.DataFrame:
    """Add 'is_reverted' column based on revert events."""
    reverted_ids = df_reverts["claim_id"]
    return df_claims.with_columns(
        pl.col("id").is_in(reverted_ids).alias("is_reverted")
    )


def compute_npi_ndc_metrics(
    df_claims: pl.DataFrame,
    output_path: Path
) -> None:
    """Compute fills, reverted, avg_price, total_price per (npi, ndc)."""
    metrics = (
        df_claims.group_by(["npi", "ndc"])
        .agg([
            pl.count().alias("fills"),
            pl.col("is_reverted").sum().alias("reverted"),
            pl.sum("price").alias("total_price"),
            pl.sum("quantity").alias("total_quantity")
        ])
        .with_columns(
            pl.when(pl.col("total_quantity") > 0)
            .then((pl.col("total_price") / pl.col("total_quantity")).round(2))
            .otherwise(0.0)
            .alias("avg_price")
        )
        .select([
            "npi", "ndc", "fills", "reverted", "avg_price",
            pl.col("total_price").round(2).alias("total_price")
        ])
        .sort(["npi", "ndc"])
    )
 
    with open(output_path, "w") as f:
        json.dump(metrics.to_dicts(), f, indent=2, default=float)
    log.info(f"NPI NDC Metrics → {output_path} ({len(metrics)} rows)")


def compute_chain_recommendations(
    df_claims: pl.DataFrame,
    df_pharmacies: pl.DataFrame,
    output_path: Path
) -> None:
    """Top 2 chains by avg unit price per drug (ndc)."""
    log.info("Computing chain recommendations")
    df_with_chain = df_claims.join(df_pharmacies, on="npi", how="left")

    chain_agg = (
        df_with_chain.group_by(["ndc", "chain"])
        .agg([
            pl.sum("price").alias("total_price"),
            pl.sum("quantity").alias("total_quantity")
        ])
        .with_columns(
            pl.when(pl.col("total_quantity") > 0)
            .then((pl.col("total_price") / pl.col("total_quantity")).round(2))
            .otherwise(None)
            .alias("avg_price")
        )
        .filter(pl.col("avg_price").is_not_null())
    )

    top_chains = (
        chain_agg.sort(["ndc", "avg_price", "chain"])
        .group_by("ndc", maintain_order=True)
        .agg(pl.struct(["chain", "avg_price"]).head(2).alias("chain"))
        .with_columns(
            pl.col("chain").list.eval(pl.element().struct.field("chain")).alias("names"),
            pl.col("chain").list.eval(pl.element().struct.field("avg_price")).alias("prices")
        )
        .with_columns(
            pl.struct(["names", "prices"]).alias("chain_struct")
        )
        .select([
            "ndc",
            pl.col("chain_struct").map_elements(
                lambda x: [{"name": n, "avg_price": float(p)} for n, p in zip(x["names"], x["prices"])],
                return_dtype=pl.Object
            ).alias("chain")
        ])
        .sort("ndc")
    )

    with open(output_path, "w") as f:
        json.dump(top_chains.to_dicts(), f, indent=2, default=float)
    log.info(f"Top Chains → {output_path} ({len(top_chains)} drugs)")


def compute_quantity_insights(
    df_claims: pl.DataFrame,
    output_path: Path
) -> None:
    """Most common quantity per drug (ndc), sorted."""
    log.info("Computing quantity insights")
    quantity_modes = (
        df_claims.group_by(["ndc", "quantity"])
        .agg(pl.count().alias("count"))
        .group_by("ndc")
        .map_groups(lambda g: g.with_columns(
            pl.when(pl.col("count") == pl.col("count").max())
            .then(pl.col("quantity"))
            .otherwise(None)
            .alias("mode_qty")
        ))
        .group_by("ndc")
        .agg(pl.col("mode_qty").drop_nulls().sort().alias("most_prescribed_quantity"))
        .filter(pl.col("most_prescribed_quantity").list.len() > 0)
        .sort("ndc")
    )

    with open(output_path, "w") as f:
        json.dump(quantity_modes.to_dicts(), f, indent=2, default=float)
    log.info(f"Quantity Insights → {output_path} ({len(quantity_modes)} drugs)")


def run_pharmacy_analytics(
    df_claims: pl.DataFrame,
    df_reverts: pl.DataFrame,
    df_pharmacies: pl.DataFrame,
    output_dir: Path
) -> None:
    """Execute full pharmacy claims analytics pipeline."""
    output_dir.mkdir(exist_ok=True)

    # Step 1: Filter + Mark
    df_claims = filter_to_pharmacy_dataset(df_claims, df_pharmacies)
    df_claims = mark_reverted_claims(df_claims, df_reverts)

    # Step 2–4: Metrics, Chain Recommendations and Quantity Insights
    compute_npi_ndc_metrics(df_claims, output_dir / "npi_metrics.json")
    compute_chain_recommendations(df_claims, df_pharmacies, output_dir / "chain_recommendations.json")
    compute_quantity_insights(df_claims, output_dir / "quantity_insights.json")

    log.info("All goals completed.")
