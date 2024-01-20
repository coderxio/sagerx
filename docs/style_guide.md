# SageRx Style Guide

## Purpose

This guide will help you understand how we structure this project such as table and schema names.

## Table Names

These also correspond to the underlying file names that create the tables. File names must be unique and correspond to the name of the model when selected and created in the warehouse.

We recommend putting as much clear information into the file name as possible, including a prefix for the layer the model exists in, important grouping information, and specific information about the entity or transformation in the model.

- _Marts_:

  - [concept]s.sql
  - Concept correctly captures the content of the table, since these are user facing this is important
  - Name should be plural

- _Intermediates_:

  - int*[entity]s*[verb]s
  - Verbs should capture the business logic or transformations conducted
  - Name should be plural

- _Staging_:

  - stg\_[source]\_\_[entity]s
  - Entity captures the data values expected
  - Name should be plural
  - Staging models are the only place we’ll reference source tables, and our staging models should have a 1-to-1 relationship to our source tables
  - Source table references should be in a CTE

- _Sources_:
  - [source]\_[content]
  - Content captures the raw data that is imported
  - Name should be singular

## Schema Names

- _sagerx_lake_: raw data from data sources and seed tables
- _sagerx_dev_: where mart tables in development live
- _sagerx_: where user-facing tables live
