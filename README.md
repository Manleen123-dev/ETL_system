# ETL Benchmarking System

This project evaluates the performance of three different ETL (Extract–Transform–Load) strategies for transferring structured data between a source SQLite database and a destination SQLite database.

The goal is to analyze how execution patterns such as sequential processing, staged execution, and parallel pipelining behave as dataset size increases.

---

## 📌 Overview

The system simulates a real-world ETL workflow where records are:

1. Extracted from a source database
2. Transformed using a fixed rule set
3. Loaded into a destination database

Each strategy implements this workflow differently, allowing us to compare execution efficiency under varying workloads.

---

## 📊 Dataset Configuration

The benchmark uses progressively increasing dataset sizes:

* 300,000 records (3L)
* 600,000 records (6L)
* 900,000 records (9L)
* 1,200,000 records (12L)
* 1,500,000 records (15L)

---

## 🧾 Schema

```text
name | roll_no | email | phone_number
```

---

## 🔄 Transformation Rules

Each record undergoes the following transformations:

* `name` → converted to uppercase
* `roll_no` → prefixed with `RN_`
* `email` → converted to uppercase
* `phone_number` → prefixed with `+91`

---

## ⚙️ ETL Strategies

### 1️⃣ Sequential Row-wise ETL (`case1_direct`)

This approach processes one record at a time.

![Image](https://images.openai.com/static-rsc-4/BRz8yb-Au2yD308jjLAy0JaXnkilY4nOVcqY7VzsZyrjidNAPK6Qyaz_a6BGEdbTXoJFKYfV1niNGjNUXukI17h97iCPjWW3vekU2TvIQq6-iWf1nJxD15txQsXO_9FfcGM827v0gmq2X7k3Rac68uZN7LJ9y7soYkNJmDKsLlOYGNUcLc6sF5E8h5Zwad69?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/26aOL_M1ct8ZklLT9AvVmk46NGbWPRwaBkGZaMYauSIH3_KjTc4bWgSv2QK20jtQQY1Y4k85l6Mfm5SrbRGuFaA6f-dwuOjIRF34q3AdPzpTCMR0Z3-Q8CxGeaFDg1in09UtwDBUe5YkJKRq7yXjkbBsQE83RSUFq1sv5i7uiAMqB1N6M2G6zoNDGBX5rtZj?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/lQ1OFyM_t6p9SE1wOQBOcY6RxOToDdC24f6wcTNLaxZpRPYX1-1wJiwotvse2i88XQ4ZbzhoHuBj8CNyJrg5aDaTW5F_5eQhT-8F45JyhIRKA80BkfmtiR1KEmgZbZqR9eqc4c0QWSpu5QoCc2ZJ6BdlCFUZ7I01QH91bIVvEwDRk0bQzn_naPW-mA3pSgWV?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/Jlh46Eg0skYvBBgLQm_zR6n0PmdWUYUtz0FsMCaC_oCpf0B7G4Jb7WTnePrfZQJx7iz-l5KZMSjUViwD6Ejy0rl87UG2xenjibjrgSjw4EYEh4r-xiOAZDceUsevqMjXjDu5DK3QpkruYH-T6obU83b_Oa8zNpaDmmbKvF3iWkos0DWrHiTP8sCmU_SdPJpZ?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/GkI2BoxD29jE8WoiV4bEh5sbh4v25IlojRd7p88zKJHZ-v9iZSDJF2-ZNtM37J_vsBUhgTrFSWcZpkntIF5uRTB4cnBOMa4PXz4S0QYczHlnDnzqdEIdYYnhzWn14GNmeznpAvY5HJc4kCGz2rbTD4eiGsUACg3sJe5umei1sQy4gLGl7VRTDGS_STKL7hNZ?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/PZcVFVcLMWtEy1nmpElb1fKQXyQVedpJXG9wodSnVbn7R9G2DRHyyWAlta3D7SqeezqGxsHiFVtn-xLyPc6ALQ0gKEYhAU9_HH7oxQ7-GPMSGKrBsWSbpS7f4JoVcXZjBv-bodH9rRiX-TOEfRHgsycjq-ZFw68YxJen00RSnfSrzXM3EUrQBHbTBetSXGBp?purpose=fullsize)

```mermaid
flowchart LR
    A[Source DB] --> B[Read Record]
    B --> C[Transform]
    C --> D[Insert Record]
    D --> B
    D --> E[Destination DB]
```

**Characteristics:**

* Minimal logic complexity
* High number of database operations
* Serves as a baseline for comparison

---

### 2️⃣ Staged ETL (`case2_staged`)

This strategy separates extraction, transformation, and loading into distinct phases.

![Image](https://images.openai.com/static-rsc-4/MUdAqhCq4JcjhStxHzR6krLnv_xxQVb0DH-7K8jMprOMTljy2ht6rS6fR2n6_yYja1zg8H_Qc2HAzFnVdrbN3L57bdQPcAdxsxVwFl1Z7Db1CVVxNHfHl2fqz_yaRIduNEvjQ3f0ZPC6qOVR3DoXERwOT1yQSkGU04p2MqhZtHwsKQoj5v03ycXanYEqKkje?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/rcdHIHf3NC8_nK01YC-MmnzEUqUFi1vfSyY8Cc9ZMHuufTqBGsbo4dBWpxJ6US7I5ZX8lwmbtu51wyeoXp-H3Q6zVACOTWTmC7iJYO4kScE2OTm00N79XpMhPjEgYa46y4r9wBYbnZSep_9Z0AgYbH4EozSTaYl_XeaoYuA6plQMxdL7Q4WuKnxFSIXMGKxm?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/kdNSkYSP_ecGU7gT5wI9uaNT00jiKNL1vv-2N-Bf9jCnRx2ByClC3VpJZPv3yFHAfWJhmuLy-s-WJMcIgBiQFvH5MJwKL2TYifxwbAJe9F72coEJIHYdYuyCiFN4oDFJ2pCrMLDTxrXp5D6IQUinI2R7OS3LPIGoqCj-8b9nQXFzC6rIQZgNSxOL7jcAhZnx?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/cTv8u5qJoO4I_vYlWSOidIxRQ12nlPRZNatehbv1M5IrYXIXKwik_hhQU36hELmeiWP7PRkmLwUeKwNGwh8YFv5jk2ENdw1rNwy1pJbC8eYiwJdK7CIgHtrWeo6BSgrXdOaix_LzbznWF5Wcq_O1wANAsfvXK6eDPvI1a8oB2qbYCGVfbkZbID-RzFOrncTb?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/ChrJy6ExUFA43SnWeyWLK3bzMKlJk1cHU6WCydX3TMw0VpzzYDsORWAuXauS3YPANLIuabd-gfiyu6Uan8zC2b9rovKTiXqiKa-NNnRQY1meYiFDL0RCddw0MJfSbwK0-HKDtcxxTHlGo0YOh-XQYIiTA1sR47AwQFuBkoRWDLGiT33ZoqVt9mNKe7wvOQqJ?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/JYtV5niUVA3BmkoULytA5Jq7RXRU1Z2eV31wJWXXFL-Wo-5R-H6EW7IfuqJNfS2oB0IZrSGfip4x6jbjInTFVO34ByXEPKmd_pXzhrBom7tMj6pCcnolo7zl_jxlYaGSgDuTOUh-RZQGfuWM-kDgLN8WYuA7knQSlLHQOE0cAbXvFezKX43crtsMz9w4LjfT?purpose=fullsize)

```mermaid
flowchart LR
    A[Source DB] --> B[Extract All Data]
    B --> C[Transform Dataset]
    C --> D[Bulk Insert]
    D --> E[Destination DB]
```

**Characteristics:**

* Reduced database round-trips
* Better batching efficiency
* No overlap between stages

---

### 3️⃣ Parallel Chunk-based ETL (`case3_parallel`)

This approach introduces chunking and concurrency using multiple threads.

![Image](https://images.openai.com/static-rsc-4/d_ryrolc-186Ry1YJKt2CYxi9aFNri1p3bA3o0IhFQhR7X8SySISxlOuqRVOPUNrR2uaZq1A-BqLG4o1ZJ2ZobS1tRxd6WKotuDzKb01pD4mVdmcgxkPiSmVV259GP6mFe4UuevyYMDLbY5Si5CpFG67z9cfrbgNlff-8QR3YCWHMesCTx10nn3HSmJXwUm0?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/AIynWNZU4up5sAVxNNy5baQ4uY-zVEiRx-8TuzpmkZc6bnk-0dqGhnMNRr4IvpfUU_54FAYFt9tzodV2mpAMxD-2asxMB3EQdZZtIoYJits5HKAYRNApYsKQetrAOeKRefVCp0YUT8z8ru0yK-Zc9Qap_KAPVLf4mM_yIKZh1RyS5-Cv8TJqWu1B5WHnIEbU?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/PC3HBF5_c4So57_7KNRYh9nDzwWPLXB9pREP135Y3C9-YUFKhz5wdZUD_CYb4_FnY-UiXLk6poQKe9XhhZ8QFmiM4kxy8vUxzkvEgCgYYWKz1T4r4aRW45vN6X4D2R-71PZK6v7IbXLfA0bfRSjsqcpKYqBqAHgYYkbulm-vBPetkR-0ybzxoChePzypaxwE?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/26aOL_M1ct8ZklLT9AvVmk46NGbWPRwaBkGZaMYauSIH3_KjTc4bWgSv2QK20jtQQY1Y4k85l6Mfm5SrbRGuFaA6f-dwuOjIRF34q3AdPzpTCMR0Z3-Q8CxGeaFDg1in09UtwDBUe5YkJKRq7yXjkbBsQE83RSUFq1sv5i7uiAMqB1N6M2G6zoNDGBX5rtZj?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/JhqL-DTIihZ_88Zz2-UBkO1nUSSjLnk5kMOyQ3uhY-Zmn5Av1rOGA1pA5ShJU8OkL0UUtw29vdsElSBrPiNCez64w68iVy_-LGJIbZZCVzEilk0Z5h4YAtXMrE_Yl7cBOoYBu0rtWi2av5HgwwbZVywNv3LvLkX4ou-07mM1AmOcooF9S_ZK9brOmix9J7gH?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/nljtg-oYqppiYhUq3yOsp245IrAFWCwcCsSHFHEgvhHTNtJluSx6Mu21Z6EfK-S-xt-uHhd1uHxhl1G1H-ZRheqRlx2BPuPkPg5ezikcpnik7U0hlZuaV80uStGTYx9837IaK2yrX4zm5l1byQM22HbxWP1PgFzvrAE8XpqJHRVYl3Nfycu0IBxsaXA3W14e?purpose=fullsize)

```mermaid
flowchart LR
    A[Source DB] --> B[Extract Thread]
    B --> C[Extract Queue]
    C --> D[Transform Thread]
    D --> E[Transform Queue]
    E --> F[Load Thread]
    F --> G[Destination DB]
```

```mermaid
sequenceDiagram
    participant Extract
    participant Transform
    participant Load

    Extract->>Transform: Chunk 1
    Extract->>Transform: Chunk 2
    Transform->>Load: Chunk 1 processed
    Load-->>Load: Insert Chunk 1
```

**Characteristics:**

* Uses chunk-based batching
* Overlaps ETL stages
* Reduces idle time between operations
* Performance depends on chunk size

---

## 🧩 Chunk Size Configuration

For the parallel pipeline, multiple chunk sizes are tested:

* 1 MB
* 2 MB
* 5 MB
* 6 MB

The optimal chunk size is determined based on execution time.

---

## 📁 Project Structure

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

---

## ▶️ Execution

### Install dependencies

```powershell
py -3 -m pip install -r requirements.txt
```

### Run full benchmark

```powershell
py -3 main.py
```

### Run with custom parameters

```powershell
py -3 main.py --sizes 300000 600000 --chunk-sizes-mb 1 2 5 6
```

### Generate dataset manually

```powershell
py -3 data_generator\generate_data.py --records 300000
```

---

## 📈 Output Artifacts

The system generates the following outputs inside the `results/` directory:

* `results.csv` → raw benchmark data
* `results_table.png` → tabular comparison
* `benchmark_plot.png` → performance visualization

---

## 📊 Results Interpretation

The generated outputs help answer:

* How does sequential ETL scale with data size?
* Does batching significantly improve performance?
* How much benefit is gained from parallel execution?
* Which chunk size yields optimal performance?

---

## 📉 Sample Results

### Result Table
\<img width="1062" height="480" alt="image" src="https://github.com/user-attachments/assets/1d4c7a79-bf6e-4a29-8091-988a2bf8a7d4" />



```text
results/results_table.png
```

### Performance Plot
<img width="1320" height="760" alt="image" src="https://github.com/user-attachments/assets/63e71618-80bb-4674-9d8c-317e8bff0435" />


```text
results/benchmark_plot.png
```

---

## 🔍 Key Observations

Typical observations from the benchmark:

* Sequential ETL becomes inefficient as dataset size increases
* Staged ETL improves performance due to batching
* Parallel ETL achieves the best performance due to overlapping stages
* Chunk size plays a critical role in optimizing throughput

---

## 🔄 Reproducibility

* Dataset generation is deterministic
* Identical chunk sizes are used across all runs
* Row counts and transformations are validated after execution

---

## 🧠 Conclusion

This project demonstrates how different ETL execution models impact performance under increasing data loads. It highlights the importance of batching and concurrency in designing scalable data pipelines.

---

