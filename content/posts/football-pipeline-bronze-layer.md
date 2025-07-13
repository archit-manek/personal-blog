+++
date = '2025-07-11T16:06:18-07:00'
draft = false
title = 'Football Data Pipeline: Bronze Layer'
categories = ['Data Engineering', 'Football Analytics']
tags = ['python', 'polars', 'data-pipeline', 'statsbomb', 'medallion-architecture', 'parquet', 'football']
+++

## Contents
- [Why I'm Building This](#why-im-building-this)
- [The Challenge: Heterogeneous Football Data](#the-challenge-heterogeneous-football-data)
- [Architecture Overview](#architecture-overview)
- [The Ingestion Pipeline](#the-ingestion-pipeline)
- [Key Design Decisions](#key-design-decisions)
- [Data Volume and Performance](#data-volume-and-performance)
- [Logging and Monitoring](#logging-and-monitoring)
- [Benefits of This Approach](#benefits-of-this-approach)
- [What I’d Do Differently (Lessons Learned)](#what-id-do-differently-lessons-learned)
- [Next Steps: Silver Layer](#next-steps-silver-layer)
- [Conclusion](#conclusion)

## Project & Connect

- [GitHub Repository](https://github.com/archit-manek/football_pipeline)
- [StatsBomb Repository](https://github.com/statsbomb/open-data)
- [LinkedIn](https://www.linkedin.com/in/architmanek/)
- [X (Twitter)](https://x.com/archit_manek)

# Building a Robust Bronze Layer: The Foundation of Football Analytics

*How I designed a scalable data ingestion system for StatsBomb football data using Python, Polars, and the medallion architecture*

## Why I'm Building This

I love football. After years of watching, debating tactics, and obsessing over matches, I wanted to go deeper - not just as a fan, but as someone who can analyze the game with real data. For me, learning data engineering and machine learning *through* football made the technical learning curve way less intimidating and a lot more fun. This project was also my way of scratching that itch: whether it’s analyzing tactics, understanding player performance, or exploring questions around injury prevention.

I set out to build a robust data pipeline for football analytics, from ingesting raw StatsBomb data to producing efficient, clean, structured datasets for analysis and modeling. My goal was to create a system flexible enough to support any question from tactics to scouting to performance analysis. In the future, I plan to incorporate more data sources to widen the possibilities even further.

In football analytics, data quality is everything. Before you can build sophisticated models or extract tactical insights, you need a solid foundation for ingesting, storing, and organizing your data. That’s where the Bronze layer comes in - the first stage of the medallion architecture, transforming raw, messy data into a structured, queryable format.

*A quick note for the uninitiated:*
A [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) is a data design pattern for logically organizing data into layers (Bronze, Silver, Gold), each building on the last.

Honestly, I first explored the medallion architecture because I had a job interview at a finance company that used it, so it seemed like a practical way to learn something relevant. But as I started working with it, I quickly saw how well it fit the challenges of structuring football data. The medallion approach naturally enforces a clean separation between raw, intermediate, and analysis ready datasets, which makes the pipeline easier to maintain, test, and extend. My main priority was to learn by building, get something working quickly, and iterate from there. So far, benefits like clear data lineage, easier debugging, and flexibility to add new sources have made it a great fit for my goals. I’m always open to changing the approach if the project’s needs evolve, but the medallion pattern has proven both practical and scalable for this work.

In this post, I’ll walk through how I built a comprehensive Bronze layer for StatsBomb football data, handling everything from match events to 360° event snapshots, across multiple competitions and seasons.

## The Challenge: Heterogeneous Football Data

Football data is notoriously complex. Unlike traditional business data, football analytics requires handling:

- **Multi-dimensional events**: Every event (pass, shot, etc.) includes spatial (x, y) coordinates, timestamps, and rich contextual info
- **Nested JSON structures**: StatsBomb's rich data format includes deeply nested objects for players, teams, tactics, and event details
- **Large-scale data**: A single match can contain thousands of events, and we're processing hundreds of matches across multiple competitions
- **360° tracking data**: Positional data for all 22 players plus the ball, captured at key events (not continuous tracking).
- **Temporal relationships**: Events are chronologically ordered, but analysts often need to group or aggregate them (using the possession field, etc.) to reconstruct possessions, phases of play, or other temporal sequences for deeper analysis.


## Architecture Overview

Here’s the folder structure I use to separate raw, untouched JSON data from the processed, schema enforced Parquet files in the Bronze layer. This organization makes it easy to keep track of what’s been ingested, and lays the groundwork for the Silver and Gold layers later on. Makes adding new competitions or sources trivial later.

```bash
data/
├── raw/
│   ├── competitions.json
│   ├── matches/
│   ├── events/
│   ├── lineups/
│   └── three-sixty/
└── bronze/
    ├── competitions/
    ├── matches/
    ├── events/
    ├── lineups/
    └── 360_events/
```

## The Ingestion Pipeline

### 1. Data Source Structure

The raw data comes from StatsBomb's [open-data](https://github.com/statsbomb/open-data) repo which is in JSON format, organized by:

- **Competitions**: Metadata about leagues, tournaments, and seasons
- **Matches**: Game level information including teams, scores, and dates
- **Events**: Detailed play-by-play data with spatial coordinates
- **Lineups**: Player formations and tactical information
- **360° Events**: Advanced positional tracking data

### 2. Core Ingestion Functions

The Bronze layer consists of five specialized ingestion functions, each handling a specific data type:

```python
def bronze_ingest():
    ingest_competitions_local()
    ingest_matches_local()
    ingest_lineups_local()
    ingest_events_local()
    ingest_360_events_local()
```

#### Events Ingestion Example
```python
def ingest_events_local():
    events_dir = Path("data/raw/events")
    bronze_events_dir = BRONZE_DIR_EVENTS
    for json_file in events_dir.glob("*.json"):
        match_id = json_file.stem
        events_path = bronze_events_dir / f"events_{match_id}.parquet"
```

Events are the heart of football analytics. Each match produces a JSON file containing hundreds or thousands of events, each with rich metadata:
- Event type (pass, shot, tackle, etc.)
- Spatial coordinates (x, y positions)
- Timestamps and possession information
- Player and team details
- Contextual information (pressure, technique, outcome)

## Key Design Decisions

### 1. File Format: Parquet over JSON

I chose Parquet for the Bronze layer for several reasons:

- **Compression**: Parquet files are typically 10-20x smaller than JSON
- **Columnar storage**: Enables efficient querying of specific columns
- **Schema enforcement**: Maintains data types and structure
- **Performance**: Faster read/write operations for large datasets

### 2. Naming Conventions

Consistent naming is crucial for data discovery and automation:

- **Events**: `events_{match_id}.parquet`
- **Matches**: `matches_{comp_id}_{season_id}.parquet`
- **Lineups**: `lineups_{match_id}.parquet`
- **360° Events**: `events_360_{match_id}.parquet`

### 3. Idempotent Operations

The ingestion functions are designed to be idempotent AND intelligent - they only reprocess files when the source data has actually changed:

```python
if parquet_path.exists() and not is_source_newer(json_file, parquet_path):
    logger.debug(f"{parquet_path} already exists and source is not newer, skipping.")
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


### 4. Error Handling

Robust error handling prevents pipeline failures, an example from ingesting events:

```python
try:
    with open(json_file, "r") as f:
        data = json.load(f)
    if data:
        df = pl.DataFrame(data)
        df.write_parquet(events_path)
        logger.info(f"Saved events for match {match_id} to {events_path}")
        processed_count += 1
    else:
        logger.info(f"No events found for match {match_id}")
except json.JSONDecodeError as e:
    logger.warning(f"Could not decode JSON for match {match_id}: {e}")
except Exception as e:
    logger.warning(f"Error processing events for match {match_id}: {e}")
```

## Data Volume and Performance

The Bronze layer handles substantial data volumes:

| Metric            | Value                                   |
|-------------------|-----------------------------------------|
| Raw JSON files    | 12.6 GB (2,200 matches, 300 KB–4 MB each) |
| Total events      | ~7–8 million rows                       |
| Bronze Parquet    | 1.53 GB (~90% compression)              |
| Performance gain  | 2x faster with Polars vs Pandas         |


When I first started, I cloned the StatsBomb open data repo locally and worked with those files. At some point, I decided to “upgrade” and pull everything directly from the StatsBomb API, thinking it would keep my data always up-to-date. But in practice, this was a pain because downloads were *so* slow, and it didn’t actually add much value for a historical analytics project. So I switched back to using local files, which turned out to be much faster and more reliable. Lesson learned: for most projects, local raw files are plenty good, and much easier to work with.

I initially built my ingestion pipeline using Pandas, but once I started processing the full StatsBomb dataset, performance was visibly slow - it took about 2 minutes to process just the events data. After switching to [Polars](https://pola.rs/) , my processing time dropped by around 50%. If you’re working with large or complex data, I highly recommend giving Polars a try, it made a huge difference in both speed and memory usage for this project.

## Logging and Monitoring

Comprehensive logging ensures visibility into the ingestion process:

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bronze/pipeline.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
```

The logging captures:
- File processing status
- Data validation results
- Error conditions and recovery
- Performance metrics

## Benefits of This Approach

### 1. Scalability
The modular design allows easy addition of new data sources or competitions without modifying existing code.

### 2. Data Quality
Parquet format enforces schema consistency and prevents data corruption during storage.

### 3. Query Performance
Columnar storage enables fast analytical queries, even on large datasets.

### 4. Maintainability
Clear separation of concerns makes the pipeline easy to debug and extend.

### 5. Reproducibility
Idempotent operations ensure consistent results across multiple runs.

## What I’d Do Differently (Lessons Learned)
### Don’t Overcomplicate Data Sources

I wasted time switching to the StatsBomb API, thinking I needed “live” data. But for most football analytics projects, cloning the open-data repo locally is simpler, faster, and good enough - especially for reproducible research and data that isn't real time.

### Optimize for Performance Early

My first Bronze layer was built in Pandas, but large-scale JSON parsing and Parquet writes were slow. Moving to Polars halved the runtime and made batch processing much smoother.

### Work on Data You Actually Care About
One of the biggest advantages was working with football data - a subject I genuinely love. It made the technical learning curve feel less intimidating, kept me motivated, and made the process much more fun. If you’re building your own projects, try to pick data you’re passionate about; it really does make a huge difference.

## Next Steps: Silver Layer

The Bronze layer provides the foundation, but the real value comes in the Silver layer where we:

- Flatten nested JSON structures
- Add derived features and calculations
- Implement data validation and quality checks
- Create standardized schemas for downstream consumption

## Conclusion

Building a robust Bronze layer is the first step toward advanced football analytics. By focusing on data quality, performance, and maintainability, we create a solid foundation that supports sophisticated analysis and machine learning models.

The key is to treat data ingestion as a first-class engineering problem, not just a preprocessing step. With proper architecture and tooling, you can handle the complexity of football data while maintaining the flexibility to adapt to new requirements and data sources.

In the next post, I'll dive into the Silver layer, where we transform this raw data into analysis-ready features and implement the data quality controls that make advanced analytics possible.

---

## Feedback
I’d love to hear your thoughts: questions, suggestions, or feedback on this pipeline (or football analytics in general) are all welcome!



*This Bronze layer processes StatsBomb data covering multiple competitions including the Champions League, Premier League, Bundesliga, and international tournaments, providing a comprehensive foundation for football analytics.* 