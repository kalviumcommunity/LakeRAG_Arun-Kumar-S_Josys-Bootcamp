 # LakeRAG — RAG Lakehouse System (Josys Bootcamp)

 A production-style Lakehouse pipeline for document ingestion, transformation, and RAG-ready retrieval.

 ## Overview

 LakeRAG is a Scala-first Lakehouse architecture designed to process unstructured documents into clean, queryable datasets used for Retrieval-Augmented Generation (RAG).

 ## System components

 - Delta Lake–based ETL (Raw → Silver → Gold)
 - Airflow orchestration
 - Embeddings + Vector Index
 - FastAPI retrieval layer
 - Dockerized deployment

 This repository contains all modules in a single monorepo.

 ## Monorepo structure

 ```text
 scala-etl/      # Scala + Spark + Delta ETL jobs
 airflow/        # Airflow DAGs (upcoming)
 vector-db/      # Embeddings + FAISS/Chroma (upcoming)
 fastapi/        # Retrieval API (upcoming)
 docker/         # Docker & Compose setup
 data/
   raw/          # Input files
   silver/       # Cleaned Delta tables
   gold/         # Curated Delta tables
 ```

 ## Current progress

 - ✅ Initial Scala ETL complete
   - Raw → Silver (cleaning, normalization)
   - Silver → Gold (aggregation)
   - Delta Lake enabled
 - Future components will be added PR-by-PR

 ## Running the ETL

 Full details are in `scala-etl/README.md`. Example:

 ```bash
 cd scala-etl
 sbt "runMain example.UserETL"
 ```

 ## Upcoming work

 - Data Quality rules
 - Chunking for document-based RAG
 - Embeddings + vector index
 - FastAPI `/search` & `/summarize`
 - Airflow orchestration
 - Docker Compose end-to-end