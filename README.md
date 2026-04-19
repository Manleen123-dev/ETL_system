# ETL Benchmarking System

This project benchmarks three ETL strategies for moving data from a source SQLite database to a destination SQLite database with a fixed transformation pipeline.

The three benchmark cases are:

1. `case1_direct`: sequential row-by-row ETL
2. `case2_staged`: full extract, transform, then bulk load
3. `case3_parallel`: chunked three-thread pipeline for extract, transform, and load

The benchmark dataset family uses:

- `3L` records
- `6L` records
- `9L` records
- `12L` records
- `15L` records

Source schema:

```text
name | roll_no | email | phone_number
```

Transform rules:

- `name` -> uppercase
- `roll_no` -> prefixed with `RN_`
- `email` -> uppercase
- `phone_number` -> prefixed with `+91`

## Project Layout

```text
config/
db/
data_generator/
etl/
results/
runtime/
utils/
main.py
requirements.txt
```

## Run

Install the dependency:

```powershell
py -3 -m pip install -r requirements.txt
```

Run the full benchmark:

```powershell
py -3 main.py
```

Run a custom benchmark:

```powershell
py -3 main.py --sizes 300000 600000 --chunk-sizes-mb 1 2 5 6
```

Generate one source dataset directly:

```powershell
py -3 data_generator\generate_data.py --records 300000
```

## Outputs

The benchmark writes these main artifacts into `results/`:

- `results.csv`
- `results_table.png`
- `benchmark_plot.png`

It also stores source benchmark databases in `runtime/datasets/` and uses `runtime/artifacts/` for temporary destination databases during execution.

## Reproducibility

- Source data generation is deterministic.
- The same shared chunk sizes are applied across all datasets.
- Each benchmark run validates row counts and transformation rules before recording the results.
