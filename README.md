# Data Validation Engine

A Scala + Apache Spark project made for validating data quality across database tables using metadata-driven rules. The application reads table definitions and validation metadata from a PostgreSQL schema, executes validation checks, and records the outcome in database logs and control tables.

## Overview

This project implements a validation pipeline for structured data tables. It is designed to validate:

- table headers against the expected schema defined in the semantic layer
- column data types and length constraints
- primary key uniqueness
- referential integrity between related columns
- functional rules such as required metadata values, naming conventions, and Excel cell bounds

The main execution flow is orchestrated in `src/main/scala/org/yiyit/App.scala`. The program loads Spark, reads the relevant metadata tables, validates each target table, and records validation progress/status in the database.

## Technology stack

- Scala 2.12
- Apache Spark 2.4.5
- Maven
- PostgreSQL JDBC driver

## Validation workflow

The application validates each table in the following order:

1. Header validation
   - checks whether the table columns match the expected names defined in `semantic_layer`
   - renames columns when the source table lacks a header row

2. Technical validation
   - checks data types and length constraints
   - verifies primary key uniqueness

3. Referential integrity validation
   - validates relationships between columns and their referenced tables

4. Functional validation
   - ensures required metadata is present for certain data
   - checks naming patterns
   - validates Excel-style cell references are within valid table bounds

5. Logging
   - updates control flags and validation logs in the database
