+++
date = '2025-07-11T16:06:18-07:00'
draft = false
title = 'Football Data Pipeline: Bronze Layer'
categories = ['Data Engineering', 'Football Analytics']
tags = ['python', 'polars', 'pandas', 'data-pipeline', 'statsbomb', 'medallion-architecture', 'parquet', 'football']
comments = true
+++

## Contents
- [Why I'm Building This](#why-im-building-this)
- [The Challenge: Heterogeneous Football Data](#the-challenge-heterogeneous-football-data)
- [Architecture Overview](#architecture-overview)
- [The Ingestion Pipeline](#the-ingestion-pipeline)
- [Key Design Decisions](#key-design-decisions)
- [Data Volume & Performance](#data-volume--performance)
- [Logging and Monitoring](#logging-and-monitoring)
- [Lessons Learned](#lessons-learned)
- [Next Steps: Silver Layer](#next-steps-silver-layer)

## Project & Connect

- [GitHub Repository](https://github.com/archit-manek/football_pipeline)
- [StatsBomb Repository](https://github.com/statsbomb/open-data)
- [LinkedIn](https://www.linkedin.com/in/architmanek/)
- [X (Twitter)](https://x.com/archit_manek)

# Building a Robust Bronze Layer: The Foundation of Football Analytics

*How I designed a scalable data ingestion system for multi-source football data using Python, Polars, Pandas, and the medallion architecture*

## Why I'm Building This

I love football. After years of watching, debating tactics, and obsessing over matches, I wanted to go deeper - not just as a fan, but as someone who can analyze the game with real data. For me, learning data engineering and machine learning *through* football made the technical learning curve way less intimidating and a lot more fun. This project was also my way of scratching that itch: whether it's analyzing tactics, understanding player performance, or exploring questions around injury prevention.

I set out to build a robust data pipeline for football analytics, from ingesting raw multi-source data to producing efficient, clean, structured datasets for analysis and modeling. My goal was to create a system flexible enough to support any question from tactics to scouting to performance analysis, while handling the complexity of multiple data vendors and formats.

In football analytics, data quality is everything. Before you can build sophisticated models or extract tactical insights, you need a solid foundation for ingesting, storing, and organizing your data. That's where the Bronze layer comes in - the first stage of the medallion architecture, transforming raw, messy data into a structured, queryable format.

*A quick note for the uninitiated:*
A [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) is a data design pattern for logically organizing data into layers (Bronze, Silver, Gold), each building on the last.

Honestly, I first explored the medallion architecture because I had a job interview at a finance company that used it, so it seemed like a practical way to learn something relevant. But as I started working with it, I quickly saw how well it fit the challenges of structuring football data. The medallion approach naturally enforces a clean separation between raw, intermediate, and analysis ready datasets, which makes the pipeline easier to maintain, test, and extend. My main priority was to learn by building, get something working quickly, and iterate from there. So far, benefits like clear data lineage, easier debugging, and flexibility to add new sources have made it a great fit for my goals. I'm always open to changing the approach if the project's needs evolve, but the medallion pattern has proven both practical and scalable for this work.

In this post, I'll walk through how I built a comprehensive Bronze layer for multi-source football data, handling everything from StatsBomb community data to professional J1 League data with HUDL tracking, demonstrating how to manage heterogeneous data sources within a unified pipeline.

## The Challenge: Heterogeneous Football Data

Football data is notoriously complex. Unlike traditional business data, football analytics requires handling:

- **Multi-dimensional events**: Every event (pass, shot, etc.) includes spatial (x, y) coordinates, timestamps, and rich contextual info
- **Nested JSON structures**: StatsBomb's rich data format includes deeply nested objects for players, teams, tactics, and event details
- **Large-scale data**: A single match can contain thousands of events, and we're processing thousands of matches across multiple competitions
- **Multiple data sources**: StatsBomb, HUDL, CSV mappings - each with different formats and schemas
- **360° tracking data**: Positional data for all 22 players plus the ball, captured at key events (not continuous tracking)
- **Temporal relationships**: Events are chronologically ordered, but analysts often need to group or aggregate them (using the possession field, etc.) to reconstruct possessions, phases of play, or other temporal sequences for deeper analysis
- **Schema variability**: Different event types can have 25-47 columns each, creating extreme variability within datasets

## Architecture Overview

The pipeline handles **two distinct data sources** with different characteristics, demonstrating how to manage heterogeneous data within a unified medallion architecture:

**1. Open Data (StatsBomb Community Data)**
```bash
data/
├── landing/
│   └── open_data/
│       ├── competitions/
│       ├── matches/
│       ├── events/
│       ├── lineups/
│       └── three-sixty/
└── bronze/
    └── open_data/
        ├── competitions/
        ├── matches/
        ├── events/
        ├── lineups/
        └── 360_events/
```

**2. J1 League (Multi-Source Professional Data)**
```bash
data/
├── landing/
│   └── j1_league/
│       ├── sb-matches/          # StatsBomb format
│       ├── sb-events/           # StatsBomb format  
│       ├── hudl-physical/       # HUDL physical data
│       └── mappings/            # CSV mapping files
└── bronze/
    └── j1_league/
        ├── matches/
        ├── events/
        ├── physical/
        └── mappings/
```

This multi-source architecture demonstrates handling heterogeneous data formats (JSON, CSV) and vendors (StatsBomb, HUDL) within a unified pipeline, while maintaining clear separation and consistent processing patterns.

## The Ingestion Pipeline

The Bronze layer processes **two distinct data sources** with specialized ingestion functions:

### Open Data Pipeline
```python
def open_data_ingest():
    """Process StatsBomb open-data repo"""
    ingest_competitions_local()
    ingest_matches_local()
    ingest_lineups_local()
    ingest_events_local()
    ingest_360_events_local()
```

### J1 League Pipeline
```python
def j1_league_ingest():
    """Process professional J1 League data from StatsBomb & Hudl"""
    ingest_j1_league_matches()    # StatsBomb format
    ingest_j1_league_events()     # Complex event data
    ingest_j1_league_physical()   # HUDL tracking data
    ingest_j1_league_mappings()   # CSV mapping files
```

## Key Design Decisions

### 1. Hybrid Approach: Pandas + Polars

Rather than a simple migration from Pandas to Polars, I implemented a **hybrid approach** that leverages the strengths of both:

```python
def create_dataframe_safely(data, target_schema: dict, logger=None):
    try:
        # 1. Pandas for JSON normalization (better nested structure handling)
        df_pd = pd.json_normalize(data)
        
        # 2. Convert to Polars for performance and schema enforcement
        df = pl.from_pandas(df_pd)
        
        # 3. Standardize column names (dots → underscores)
        df = normalize_column_names(df)
        
        # 4. Apply schema with Polars' efficient type system
        return apply_schema_flexibly(df, target_schema, logger)
    except Exception as e:
        # Fallback: Polars only processing with string inference
        if logger:
            logger.warning(f"Pandas failed: {e} | Attempting Polars fallback")
        df = pl.DataFrame(data, infer_schema_length=0)
        df = normalize_column_names(df)
        return apply_schema_flexibly(df, target_schema, logger)
```

**Why this hybrid approach works:**
- **Pandas** json_normalize – safest way to flatten StatsBomb’s tangled JSON. Dependable way to turn deeply nested columns into flat columns
- **Polars** (after flatten) – much faster and lighter for all later filtering, joins, and Parquet work.



### 2. File Format: Parquet over JSON

I chose Parquet for the Bronze layer for several reasons:

- **Compression**: Parquet files are typically 10-20x smaller than JSON
- **Columnar storage**: Enables efficient querying of specific columns
- **Schema enforcement**: Maintains data types and structure
- **Performance**: Faster read/write operations for large datasets

### 3. Naming Conventions

Consistent naming is crucial for data discovery and automation:

- **Events**: `events_{match_id}.parquet`
- **Matches**: `matches_{comp_id}_{season_id}.parquet`
- **Lineups**: `lineups_{match_id}.parquet`
- **360° Events**: `events_360_{match_id}.parquet`
- **Physical**: `hudl_physical.parquet`
- **Mappings**: `{type}_mapping.parquet`

### 4. Idempotent Operations

The ingestion functions are designed to be idempotent AND intelligent - they only reprocess files when the source data has actually changed:

```python
if output_path.exists() and not is_source_newer(json_file, output_path):
    logger.info(f"{output_path} already exists and source is not newer, skipping.")
    continue
```

```python
def is_source_newer(source_path: Path, output_path: Path) -> bool:
    """Check if source file is newer than output file."""
    if not output_path.exists():
        return True
    return source_path.stat().st_mtime > output_path.stat().st_mtime
```

This ensures the pipeline can be safely re-run for incremental updates or error recovery.

### 5. Enhanced Error Handling

The pipeline implements **multi-layered error recovery**:

```python
def create_dataframe_safely(data, target_schema: dict, logger=None):
    try:
        # Primary: Pandas json_normalize + Polars processing
        df_pd = pd.json_normalize(data)
        df = pl.from_pandas(df_pd)
        df = normalize_column_names(df)
        return apply_schema_flexibly(df, target_schema, logger)
    except Exception as e:
        # Fallback: Polars-only processing with string inference
        if logger:
            logger.warning(f"Pandas failed: {e} | Attempting Polars fallback")
        df = pl.DataFrame(data, infer_schema_length=0)
        df = normalize_column_names(df)
        return apply_schema_flexibly(df, target_schema, logger)
```

This approach ensures **robust data processing** even with malformed or unexpected JSON structures, while providing clear logging for debugging.

## Schema Standardization Challenge

**The Problem:**
- Pandas `json_normalize` creates: `'home_team.home_team_name'` (dots)
- Polars schemas expect: `'home_team_home_team_name'` (underscores)
- Result: Schema mismatches causing data loss and null values

**The Solution:**
```python
def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """Convert dot notation to underscore notation for consistent schema application."""
    rename_map = {col: col.replace('.', '_') for col in df.columns}
    return df.rename(rename_map)
```

## Data Volume & Performance Snapshot

| Metric                        | Open-Data (community) | J1 League (pro) | Combined |
|-------------------------------|----------------------:|----------------:|---------:|
| Raw landing size (JSON/CSV)   | **12.62 GB**          | **1.78 GB**     | 14.40 GB |
| Bronze size (Snappy Parquet)  | **1.64 GB**           | **4.8 MB**      | 1.64 GB* |
| Compression vs. raw           | ~ **-87 %**           | ~ **-99.7 %**   | ~ -89 % |
| Event rows                    | ≈ 7 M                | ≈ 1.2 M         | ≈ 8.2 M |
| HUDL physical rows            | —                    | ≈ 1.06 M        | ≈ 1.06 M |

\* J1 League’s Bronze footprint rounds to zero at GB scale.

**Key take-aways**

- **Hybrid flow:** `pandas.json_normalize` for the initial flatten → Polars for Parquet writes and all downstream transforms.  
- **Speed:** Post-flatten transforms run ~2× faster in Polars than in pure pandas, with lower RAM usage.  
- **Storage:** Columnar Snappy Parquet cuts raw size by 87–99 %, keeping a 14 GB landing zone to ~1.6 GB on disk.  
- **Incremental runs:** If no files changed, Bronze refresh completes in seconds thanks to the source-timestamp check.


## Logging and Monitoring

Comprehensive logging ensures visibility into the ingestion process, an example:

```python
# Open Data logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bronze/bronze_open_data.log", mode="w"),
        logging.StreamHandler()
    ]
)
```

The logging captures:
- File processing status and progress
- Data validation results
- Error conditions and recovery attempts
- Performance metrics and timing
- Schema application warnings

## Benefits of This Approach

### 1. Multi-Source Scalability
The modular design allows easy addition of new data sources (HUDL, Opta, etc.) without modifying existing code, demonstrated by the seamless integration of J1 League data alongside StatsBomb.

### 2. Data Quality Assurance
- Parquet format enforces schema consistency
- Hybrid processing handles malformed data gracefully
- Column name standardization prevents downstream errors

### 3. Query Performance
Columnar storage enables fast analytical queries, even on datasets with millions of events.

### 4. Maintainability
- Clear separation of concerns between data sources
- Shared utilities for common operations
- Comprehensive error handling and logging

### 5. Reproducibility
Idempotent operations ensure consistent results across multiple runs while supporting incremental updates.

## Lessons Learned

### Keep It Simple On Purpose
At the start I felt inclined to pull data straight from every API and to automate “everything.”
That quickly turned into long waits, flaky downloads, and wasted evenings debugging endpoints I didn’t even need.
Next time: clone the public repo first, get a small slice working end-to-end, and only add new sources when a real question demands it.

### Work on Data You Actually Care About
One of the biggest advantages was working with football data - something I genuinely love. It made the technical learning curve feel less intimidating, kept me motivated, and made the process much more fun. If you’re building your own projects, try to pick data you’re passionate about; it really does make a huge difference.

### Importance Of a Strong Foundation
Once open-data was running smoothly, I added the free J1 League + HUDL physical dataset to see how the pipeline would handle a second source. That quick test showed me the gaps: my folder layout was ad-hoc, helper code was duplicated, and naming wasn’t consistent. I paused, reorganised the directory structure, and pulled common logic into reusable functions. With those basics in place, adding new files is straightforward instead of confusing. Small reminder to myself: spend a little time on the groundwork before the next data drop arrives.

## Next Steps: Silver Layer

The Bronze layer provides the foundation, but the real value comes in the Silver layer where we:

- Process complex event data with event-type-specific schemas
- Add derived features and calculations
- Implement advanced data validation and quality checks
- Create standardized schemas for downstream consumption
- Handle the extreme variability in event data structures

---

## Feedback
I'd love to hear your thoughts: questions, suggestions, or feedback on this pipeline (or football analytics in general) are all welcome!

*This Bronze layer processes StatsBomb data covering multiple competitions including the Champions League, Premier League, Bundesliga, and international tournaments, plus J1 League data with HUDL tracking, providing a comprehensive foundation for multi-source football analytics.* 