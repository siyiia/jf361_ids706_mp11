# jf361_ids706_mp11
[![CI](https://github.com/siyiia/jf361_ids706_mp11/actions/workflows/cicd.yml/badge.svg)](https://github.com/siyiia/jf361_ids706_mp11/actions/workflows/cicd.yml)

## Project Introduction
This project is to use Data Pipeline with Databricks.

## Project Requirments
- Create a data pipeline using Databricks
- Include at least one data source and one data sink

## Project Output
The markdown report can be found here [Result Report](./result_report.md).

## Setup
1. Create a Cluster in `Databricks`.
2. Under personal workspace, import a GitHub repo
3. Under `Job Runs`, click `Create job` to create a pipeline for all jobs needed for this project.
4. Then click `Run` to run the jobs.

## Project Description
1. Extract data from a specified URL and save it as a `.csv` file in `Databricks File System (DBFS)`.
2. Load the `.csv` file from DBFS into a Spark `DataFrame` for processing.
3. Apply transformations and modifications to the `DataFrame` to prepare the data.
4. Save the transformed `DataFrame` back to `Databricks` as a managed table.
5. Execute SQL queries on the table to analyze or retrieve specific data.


## Workflow
<p>
    <img src="screenshots/workflow.png" />
</p>