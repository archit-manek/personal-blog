+++
date = '2025-07-21T16:06:18-07:00'
draft = false
title = 'Pandas vs Polars for Football Event EDA: A Benchmark'
slug = 'pandas-vs-polars-football-eda'
categories = ['Data Engineering', 'Football Analytics']
tags = [
    'python',
    'polars',
    'pandas',
    'eda',
    'benchmarking',
    'data-pipeline',
    'football',
    'sports-analytics',
    'parquet',
    'performance'
]
comments = true
+++

## Contents

- [Football Event Data: Scale and Shape](#football-event-data-scale-and-shape)
- [Pandas: Starting Point](#pandas-starting-point)
- [Switching to Polars: Columnar and Multi-Threaded by Design](#switching-to-polars-columnar-and-multi-threaded-by-design)
- [Benchmarks: Pandas vs Polars](#benchmarks-pandas-vs-polars)
- [Key Takeaways](#key-takeaways)
- [Feedback](#feedback)

---

## Football Event Data: Scale and Shape

In football analytics, even open event data quickly adds up:

- ~3,400 parquet files (one per match or competition)
- ~12 million rows
- ~120 columns, often with missing, null, or nested values

StatsBomb open data is wide, uneven, and forces your data tools to do real work.

---

## Pandas: Starting Point

Pandas is still the most popular for most ad hoc data analysis.  
It has a clean API, the best documentation, and deep integration with the PyData stack.

But it has two known pain points for data at this scale:

1. **I/O and concat:** Loading many Parquet files and aligning columns with NaNs is single-threaded and memory-heavy.
2. **Operations on wide frames:** Most Pandas methods (especially `describe()`, groupbys, and joins) are much slower as the number of columns and rows increases.

**Example:**

```python
import pandas as pd
from pathlib import Path
from utils.constants import get_open_data_dirs

OPEN_DATA_DIR = get_open_data_dirs()
bronze_dir = OPEN_DATA_DIR["bronze_events"]
files = list(Path(bronze_dir).glob("*.parquet"))

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
print(df.shape)
print(df.describe())
```

* **Read + concat time:** 44 seconds on my machine
* **Other DataFrame operations:** `.describe()` on this size took minutes, sometimes longer

> **Technical Note:**
> `.describe()` is especially slow in Pandas on wide, null-heavy DataFrames because it computes summary statistics (count, mean, std, quartiles, etc.) *for every column*, allocating temporary arrays and iterating column-wise in pure Python/C. The process is not multi-threaded, and is heavily impacted by both the number of columns and the presence of NaNs. For “messy” event data, this becomes a major bottleneck.

---

## Switching to Polars: Columnar and Multi-Threaded by Design

Polars is a newer DataFrame library built in Rust and based on Apache Arrow.

* Multi-threaded by default (across both I/O and compute)
* Designed for lazy evaluation and query optimization
* Handles missing columns, unions, and nested types with less overhead

**Same workflow:**

```python
import polars as pl

df = pl.scan_parquet(
    f"{bronze_dir}/*.parquet",
    extra_columns="ignore",
    missing_columns="insert"
).collect()
print(df.shape)
print(df.describe())
```

* **Load time:** \~3 seconds (over 10x faster than Pandas in my tests)
* **`.describe()`:** Less than 1 second on the same data (Polars parallelizes numeric aggregations)

> **Technical Note:**
>
> * **Lazy vs eager:** Keeping analysis lazy until it's absolutely needed avoids a full collect if you’re chaining filters. Here, `.describe()` is run eagerly after collection, so all data is materialized.
> * **I/O concurrency:** Polars reads multiple files simultaneously and only loads the data you actually use, keeping memory usage low.
> * **Null handling:** Polars processes missing data in batches, making it much faster than checking each value individually.

---

## Benchmarks: Pandas vs Polars

| Task                    | Pandas        | Polars        |
| ----------------------- | ------------- | ------------- |
| Load all files          | 44 sec        | 3 sec         |
| `.describe()`           | Minutes+      | <1 sec        |
| Handles missing columns | Yes, but slow | Yes, and fast |
| Peak RAM usage*      | 13.8 GB         | 7.1 GB       |

*Measured with `psutil` in Jupyter (cold cache):
* Dataset = 3 433 StatsBomb Parquet files (12 M × 149).  
* Hardware: 12-core M3 Pro, 36 GB RAM, macOS 14.5.  
* *pandas* 2.3.0 (`engine="pyarrow"`), *polars* 1.31.0, *pyarrow* 20.0.0.  
* Polars RSS before `.collect()` was ≈ 0.1 GB.

### Memory usage

Polars used less RAM for the same data (measured with `gtime -v python script_name.py`). \
Need to install GNU Time before running: `brew install gnu-time`

### Developer friction

Polars handled nulls and missing columns natively, without me needing to pre-align anything. 

With Pandas, if your Parquet files don’t have exactly the same columns (common in event data), you have to do the alignment yourself.

*Pandas Code Example*:
```python
import pandas as pd
import numpy as np

dfs = [pd.read_parquet(f) for f in files]
all_columns = set.union(*(set(df.columns) for df in dfs))
for df in dfs:
    for col in all_columns:
        if col not in df.columns:
            df[col] = np.nan
df = df[sorted(all_columns)]
df = pd.concat(dfs, ignore_index=True)
```

What’s happening here:
* You read each file.
* Build a master list of all columns found.
* For each DataFrame, insert any missing columns (fill with NaN) and reorder so all DataFrames match.
* Only then can you safely concatenate.

Drawbacks:
* More code, more places for mistakes, more to maintain.
* Easy to get column order or types wrong.

*Polars Code Example:*
```python
import polars as pl

# Note: Call .collect() to materialize the DataFrame before timing,
# so the timing is a fair comparison with Pandas.
df = pl.scan_parquet(
    "bronze_events/*.parquet",
    extra_columns="ignore",
    missing_columns="insert"
).collect()
```

What’s happening here:

* Polars scans all files and builds a unioned schema under the hood.
* Any missing column in a file appears as null (no code needed).
* Any extra columns are dropped.
* You call .collect() and get a unified DataFrame, ready to use.
---

## Key Takeaways

* Pandas remains the best choice for small-to-medium data or for final, presentation-ready transformations.
* Polars is my go-to for wide, multi-file, or messy football event data. Its speed and schema flexibility turn previously frustrating workflows into straightforward, interactive analysis.
* What stood out to me is how similar Polars feels to SQL in both syntax and mental model, and how efficiently it manages memory by only materializing data when needed. This design makes complex queries and large-scale data exploration feel intuitive and fast, even on a laptop.


---

## Feedback


I’d love to hear your thoughts: questions, suggestions, or feedback on this pipeline (or football analytics in general) down below.





