# Pharmacy Claims Analytics

A high-performance, production-ready analytics pipeline for pharmacy claims processing.

**Goals**:
1. Load pharmacy, claims, and revert events
2. Compute `(npi, ndc)` metrics
3. Recommend top 2 chains per drug
4. Find most common quantity per drug

---

## Requirements

Typer  - CLI tool

Polars - Data manipulation, 10x-100x faster than Pandas. Auto multi-thread.


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

