
# Project SAVE Armenian Photograph Archives — Metadata Scraper

This repository contains a scraper and dataset created as part of an **Open Data Armenia initiative**.

It compiles publicly accessible **metadata records** from the online catalog of:

Project SAVE Armenian Photograph Archives, Inc.  
https://www.projectsave.org

## Important Disclaimer

This project is **not affiliated with Project SAVE Armenian Photograph Archives, Inc.**

This repository contains **metadata only** extracted from publicly accessible catalog pages.

**No photographs or images are included.**

All rights to the original photographs remain with **Project SAVE Armenian Photograph Archives, Inc.** and their respective rights holders.

## Dataset

Files:

data/projectsave_photos.csv  
data/projectsave_photos.jsonl

Encoding: UTF-8

## Running the scraper

Install dependencies:

pip install -r requirements.txt

Run:

python src/scrape_projectsave.py

Example test run:

python src/scrape_projectsave.py --limit 50

## Licensing

Code → MIT License  
Dataset (metadata only) → CC BY 4.0
