 # LakeRAG — Scala ETL Module

 Delta Lake ETL pipelines for Raw → Silver → Gold layers.

 ## Overview

 This folder contains the Scala + Spark + Delta Lake ETL pipelines used in the LakeRAG system. The ETL flow converts raw input files into cleaned Silver datasets and aggregated Gold datasets that will later feed retrieval and RAG components.

 ## Features (Current Progress)

 - ✅ Raw → Silver ETL (`UserETL`)
   - Reads raw CSV/text input
   - Cleans and normalizes fields
   - Fills missing values
   - Writes cleaned data into Delta Lake Silver tables

 - ✅ Silver → Gold ETL (`UserAggregationETL`)
   - Performs analytical transformations
   - Generates aggregated Delta Lake Gold tables
   - Example: city-wise user counts

 ## Folder structure

 ```text
 scala-etl/
   ├── src/
   │    └── main/scala/example/
   │          ├── UserETL.scala
   │          └── UserAggregationETL.scala
   ├── build.sbt
   ├── README.md   <-- this file
       data/
           raw/
           silver/
           gold/
 ```

 ## Tech stack

 - Scala 2.12
 - Apache Spark 3.4
 - Delta Lake 2.4
 - SBT

 (Upcoming: Airflow, Vector DB, FastAPI, Docker)

 ## Running the ETL jobs

 Raw → Silver

 ```bash
 sbt "runMain example.UserETL"
 ```

 Silver → Gold

 ```bash
 sbt "runMain example.UserAggregationETL"
 ```

 ## Outputs

 - Outputs appear in `data/silver/` and `data/gold/` respectively.
 - Both layers are stored in Delta Lake (Delta Lake tables).