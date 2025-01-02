# Spotify-End-to-End-Data-Pipeline-Project
This repository demonstrates a fully automated data engineering pipeline that extracts, processes, and analyzes Spotify's Global Top Songs using APIs and AWS services. The project illustrates how modern cloud-native tools can enable seamless ETL workflows and facilitate advanced analytics.

## Project Overview
This project uses the Spotify API to retrieve real-time music data, processes and transforms it, and prepares it for analytics. Key components include automated extraction, transformation, storage, and querying capabilities, all built on AWS services.

## Pipeline Architecture
### Data Extraction:

Extract Spotify Global Top Songs via Spotify API.
Convert the data into structured formats using Pandas.
Deploy data extraction logic on AWS Lambda, enabling automated and scalable extraction workflows.

## Data Storage:

### Store extracted data in AWS S3:
Raw Data: Segmented into to-process and processed folders for easy management.
Transformed Data:Organized into structured tables (songs, artists, albums).
## Data Transformation:

Implemented a transformation Lambda function (spotify-transformation):
Automatically triggered on new uploads to the to-process folder.
Transforms raw data into relational tables ready for analytics.
Moves processed data to the appropriate S3 folder.

## Data Cataloging and Analytics:

AWS Glue: Crawlers dynamically create a data catalog for the songs, artists, and albums tables.

AWS Athena: Query transformed data directly for insights and analysis.
Technologies and Tools Used

Spotify API: Data source for global top songs.

Python: Data extraction and transformation using Pandas.

AWS Lambda: Serverless functions for automation of extraction and transformation.

AWS S3: Storage of raw and processed data.

AWS Glue: Automatic schema detection and data catalog creation.

AWS Athena: Serverless SQL querying for analytics.

## Project Features

Automated ETL Pipeline: Fully automated pipeline triggered by events.

Scalable Design: Leverages AWS Lambda for cost-effective and scalable computation.

Data Organization: Logical folder structure in S3 for easy data management.

Real-Time Transformation: Processes data as soon as it is ingested.

Advanced Querying: Use Athena for SQL-based queries and insights.
