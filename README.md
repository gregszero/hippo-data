# Pharmacy Claims Analytics

A high-performance, production-ready analytics pipeline for pharmacy claims processing.

**Goals**:
1. Load pharmacy, claims, and revert events
2. Compute `(npi, ndc)` metrics
3. Recommend top 2 chains per drug
4. Find most common quantity per drug

---

## Project Structure

hippo-data/

├── main.py              # CLI entrypoint (Typer)

├── loader.py            # Data loading (CSV/JSON)

├── processor.py         # Goals 2, 3, 4

├── logger.py            # Logging setup

├── requirements.txt

├── README.md

└── output/              # Generated JSONs + log


## Requirements

Polars
Typer


## Why Polars?

| Feature | Benefit |
|-------|--------|
| **10–100x faster than Pandas** | Processes 10M+ claims in seconds |
| **Auto multi-threaded** | Uses all **10 cores** on the instance |
| **Memory efficient** | Columnar + Arrow format |
| **Lazy evaluation** | Ready for 100M+ rows (just add `.lazy()`) |
| **Native CSV + JSON** | `pl.read_csv()`, `pl.read_json()` |
| **SQL-like API** | Clean, readable code |
| **Schema enforcement** | Catches invalid data early |

> **Allowed for Goals 3 & 4** → used for **all goals** for consistency and performance


## Install

```bash
 pip install -r requirements.txt
```

## How to Run

```bash
python main.py \
  --pharmacy data/pharmacies \
  --claims data/claims \
  --reverts data/reverts \
  --output-dir output

```

```
```
