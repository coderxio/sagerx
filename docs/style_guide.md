# SageRx Style Guide

## Purpose

This guide will help you understand how we structure this project such as table and schema names.

## Table Names

These also correspond to the underlying file names that create the tables. File names must be unique and correspond to the name of the model when selected and created in the warehouse.

We recommend putting as much clear information into the file name as possible, including a prefix for the layer the model exists in, important grouping information, and specific information about the entity or transformation in the model.

**Marts**:

- Name format: [concept]s.sql
- Concept correctly captures the content of the table, since these are user facing this is important
- Name should be plural

**Intermediates**:

- Name format: int*[entity]s*[verb]s
- Verbs should capture the business logic or transformations conducted
- Name should be plural

**Staging**:

- Name format: stg\_[source]\_\_[entity]s
- Entity captures the data values expected
- Name should be plural
- Staging models are the only place we’ll reference source tables, and our staging models should have a 1-to-1 relationship to our source tables
- Source table references should be in a CTE

**Sources**:

- Name format: [source]\_[content]
- Content captures the raw data that is imported
- Name should be singular

## Schema Names

**sagerx_lake**

- Contains raw data from data sources and seed tables, users can also access these tables to manipulate the data for their use cases.

**sagerx_dev**

- Contains tables in development live and in-progress data can be stored.

**sagerx**

- Contains user-facing tables live, these are expected to be "production" ready data.

## DAG Philosophy

Our use case of airflow DAGs is to download the data and upload it to our database, this commonality has allowed us to create layers of abstraction.

**airflow operator**

- Creates a DAG with standard parameters.

**sagerx**

- Project specific functions on how to interact with the project and its data.
- Add here common ways to interact with the project or the data.

**common_dag_tasks**

- Common operations performed by Airflow DAGs.
- Useful in defining how we have standardized the way that DAGs run.
- Add tasks here that abstract away common airflow operations.

**user_macros**

- Common functions used to manipulate data.
- Add here common ways to process data.
