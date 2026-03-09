# Maybe3rdEye

Maybe3rdEye is a hybrid reconnaissance crawler for Windows.

It combines:
- Native .NET crawling
- Scrapy-based crawl orchestration
- theHarvester source enrichment
- Real-time desktop UI control

## What It Does
- Crawls seed URLs with configurable concurrency, depth, and delay.
- Extracts and validates emails from page content.
- Streams live crawl progress to a WinForms dashboard.
- Enriches targets with theHarvester discovery modules before crawl.
- Exports discovered emails and metadata to CSV.

## Core Architecture
- `Email-Scrape/Form1.cs`
  - Desktop control plane and runtime orchestration.
- `Email-Scrape/EmailCrawler.cs`
  - Native .NET email crawler engine.
- `Email-Scrape/ScrapyEmailCrawler.cs`
  - Bridge from C# to Python worker process.
- `Email-Scrape/Python/scrapy_email_worker.py`
  - Scrapy worker + theHarvester integration pipeline.

## Crawl Engines
### 1) Native Engine (.NET)
- Fast local crawling in C#.
- Good fallback when Python stack is unavailable.

### 2) Scrapy Engine (Python)
- Uses Scrapy scheduling, throttling, dedupe filters, and depth controls.
- Supports optional pre-crawl enrichment from theHarvester sources.

## theHarvester Integration
The worker can import and run discovery modules (for example):
- `crtsh`
- `rapiddns`
- `waybackarchive`
- `commoncrawl`
- `thc`
- `duckduckgo`

Discovered hosts are converted into additional crawl seeds and discovered emails are injected into result streams.

## Requirements
- Windows
- .NET 8 SDK
- Python 3.12+ (3.14 tested)

## Setup
### .NET
```powershell
dotnet build Email-Scrape.sln -c Release
```

### Python (recommended full stack)
```powershell
python -m pip install -r .\Email-Scrape\Python\requirements-super.txt
```

### Python (manual)
```powershell
python -m pip install scrapy
python -m pip install -e .\external\theHarvester
```

## Run
```powershell
dotnet run --project .\Email-Scrape\Email-Scrape.csproj
```

## UI Controls
- Seed URLs (one per line)
- Max pages
- Threads
- Timeout
- Depth
- Delay (ms)
- Stay on seed hosts only
- Use Scrapy engine
- Enable theHarvester enrichment
- Harvester sources (comma-separated)

## Output
CSV fields:
- Email
- Hits
- FirstSeen
- LastSeen
- SourceUrl

## Security and Legal
- Respect robots.txt, target ToS, and applicable laws.
- Use only on systems/domains you are authorized to test.
- theHarvester is GPL-2.0-only; evaluate license obligations before redistribution.

## Project Status
This repository has been refactored into a modular, multi-engine reconnaissance system and is actively ready for further hardening.
