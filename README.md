 AI-Assisted Development Demo — NAPA Case Study

**DSB6000 | Module 2: Technical Leadership**

## About this repository

This repository contains the **starting materials** for the NAPA AI-assisted development lab. The goal is to gain practical experience using an AI coding assistant to turn data requirements into a working, tested data pipeline. You will work with sample player and regional data, create Databricks jobs, and build data products using a medallion architecture.

**AI is your development assistant, not the decision-maker.** You are responsible for understanding the requirements, reviewing proposed changes, checking data quality, and confirming that the finished solution works.

**Where are the instructions?** Use the separate **DSB6000 NAPA Student Lab Tutorial** provided by your instructor for the step-by-step activities, commands, Codex prompts, and checkpoints. This README is a guide to the repository itself, not a second set of lab instructions.

## What is included

The starting repository is deliberately small. It contains the raw sample data and specifications, **not a completed pipeline**.  You will use the AI Assistant to construct the pipeline.

```text
AI_Dev_Demo/
├── README.md
├── .gitignore
├── docs/
│   ├── player_master_data_dictionary.md
│   └── regions_data_dictionary.md
└── data/
    └── raw/
        ├── player_master.parquet
        └── regions.parquet
```

| File or folder | What it is used for |
| --- | --- |
| `README.md` | Explains the purpose, contents, and conventions of this repository. |
| `.gitignore` | Identifies local or generated files that Git should not track. |
| `docs/player_master_data_dictionary.md` | Describes the fields, data types, and meaning of the player master dataset. |
| `docs/regions_data_dictionary.md` | Describes the region reference dataset and its fields. |
| `data/raw/player_master.parquet` | NAPA 5K sample player data (approximately 5,000 players). |
| `data/raw/regions.parquet` | Region reference data used with the player data. |

**Parquet** is a compact, column-oriented data file format. **Markdown** (`.md`) is a plain-text format used here for project documentation and data specifications. The dictionaries are essential references: use them rather than guessing column names, data types, or relationships.

## How the project will grow

During the lab, you will use Codex and Databricks to develop the solution in stages:

**Raw Parquet files → Bronze Delta tables → Silver data product**

- **Raw:** Original input files, kept unchanged.
- **Bronze:** Delta tables for `player_master` and `regions`, preserving source information.
- **Silver:** A combined player data product based on a reviewed design, including explicit data-quality checks.

As you progress, new files and directories may appear. For example:

| Possible addition | Purpose |
| --- | --- |
| `databricks.yml` | Root configuration for a Databricks Asset Bundle. |
| `resources/` | Definitions for Databricks jobs and other deployable resources. |
| `src/` or `notebooks/` | Python/PySpark processing code. |
| `tests/` | Checks that help validate code and expected behavior. |
| `docs/player_silver_profile.md` | The reviewed design specification for the silver data product. |

These are **examples of expected development artifacts**, not files that must already exist in the starter repository. Exact filenames and organization may differ depending on the implementation you review and approve.

## Important distinctions

**GitHub is not Databricks.** Cloning this repository puts the files on your local computer; it does not upload the Parquet data into your Databricks workspace. The tutorial explains how to transfer the raw data to a Unity Catalog volume before running the pipeline.

**Databricks is the runtime environment.** The code that you construct will run in the Databricks platform in the cloud, within the workspace assigned to your Databricks Free Edition account.  You will NOT run code locally on your computer.  Your local machine is used only for maintaining a working copy of the repo and managing changes to GitHub.

**Saving code is not running code.** Git commits and pushes preserve your work in GitHub. Deploying a Databricks Asset Bundle publishes code and job definitions to Databricks. Executing a Databricks job processes the data. These are different activities.

## Good repository habits

- **Preserve the original data.** Do not overwrite, rename, or silently correct the files in `data/raw/`.
- **Use the dictionaries.** Confirm field definitions and relationships before building transformations.
- **Review AI-generated changes.** Ask Codex to explain its plan and inspect the changes before accepting them.
- **Keep a history of progress.** Make descriptive Git commits as each part of the solution is completed and verified.
- **Protect credentials.** Never commit passwords, access tokens, private keys, or Databricks authentication files.
- **Keep the repository understandable.** Use meaningful filenames and document important design decisions, assumptions, and data-quality findings.

**Need to know what to do next?** Return to the **DSB6000 NAPA Student Lab Tutorial** and continue from the next checkpoint. That document is the authoritative source for the lab procedure and instructor-specific information.

## Player master Bronze job

The bundle now defines the serverless job `player_master_bronze` in
`resources/player_master_bronze.job.yml`. Its Python source notebook is
`src/player_master_bronze.py`; `databricks.yml` defines the `dev` target using
CLI profile `AI_DEMO`. No cluster configuration or extra dependencies are needed.

The notebook reads only
`/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/player_master.parquet`
and writes the managed Delta table `ai_dev_class_demo.bronze_layer.player_master`.
It preserves all 20 documented source columns in dictionary order, including
string dates and the Parquet decimal precisions/scales. It adds no columns and
does not alter or delete the raw file.

This is a full-table overwrite of a complete input, not incremental ingestion.
Rerunning with unchanged input produces the same logical contents without
appending duplicate rows. Monthly incremental files must not replace this input
unless replacing Bronze with only those changes is explicitly intended.
The input must remain unchanged during a run. Avoid other writers to the target.

Before writing, the notebook checks column names/types/scales, required-field
null counts, unique player keys, and non-empty input. Existing targets must be
Delta tables with matching column order and types; automatic schema evolution
is disabled. Validation failure stops before the table write. Source values are
not repaired, filtered, cast, or deduplicated. Region relationships and business
quality checks are deferred to Silver.

Job output logs the source path, target, schema, row counts, validation counts,
and completion or exception details without printing player records. After the
write, it checks the Delta format, schema, and row count. A post-write failure
fails the job but does not automatically roll back the committed table.

### Validate, deploy, and run

Use an installed Databricks CLI with the authenticated `AI_DEMO` profile.
The workspace must support serverless jobs. The execution identity needs
`USE CATALOG`, `USE SCHEMA`, and `READ VOLUME` on the source; access to the
target catalog/schema and `CREATE TABLE`; and `MODIFY`/`SELECT` on an existing
target. Creating the Bronze schema also requires `CREATE SCHEMA` on the catalog.

From the repository root, validate the bundle (this does not execute ingestion):

```powershell
databricks bundle validate -t dev --profile AI_DEMO
```

After reviewing the changes and authorizing deployment, publish the job:

```powershell
databricks bundle deploy -t dev --profile AI_DEMO
```

After separately authorizing execution, run the job:

```powershell
databricks bundle run -t dev --profile AI_DEMO player_master_bronze
```

Review the run output and verify the resulting table in Databricks:

```sql
DESCRIBE TABLE ai_dev_class_demo.bronze_layer.player_master;
DESCRIBE DETAIL ai_dev_class_demo.bronze_layer.player_master;
SELECT COUNT(*) FROM ai_dev_class_demo.bronze_layer.player_master;
SELECT player_id, COUNT(*) AS occurrences
FROM ai_dev_class_demo.bronze_layer.player_master
GROUP BY player_id HAVING COUNT(*) > 1;
```

Compare the row count with the logged source count. The duplicate-key query
should return no rows. No fixed player count is assumed.
