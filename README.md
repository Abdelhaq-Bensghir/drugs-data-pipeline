# Drug-Journal Mentions Data Pipeline

This Python pipeline processes scientific publications and drug lists to identify drug mentions in titles, outputting a structured JSON graph of these connections.

We added a subfolder `./sql/` for the part II of the project.

---

## Table of Contents

- [Drug-Journal Mentions Data Pipeline](#drug-journal-mentions-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Design Philosophy](#design-philosophy)
  - [Project Structure](#project-structure)
  - [Data Sources](#data-sources)
  - [Installation](#installation)
  - [Pipeline Execution](#pipeline-execution)
  - [Output Structure](#output-structure)
    - [Why a Flat JSON Format?](#why-a-flat-json-format)
  - [Ad-hoc Analyses](#ad-hoc-analyses)
  - [SQL Queries](#sql-queries)
  - [Unit Tests](#unit-tests)
  - [Contribution](#contribution)
  - [License: MIT License.](#license-mit-license)
  - [Scalability \& Improvements](#scalability--improvements)
    - [Operational Enhancements](#operational-enhancements)
    - [Data Format Upgrades](#data-format-upgrades)
    - [Distributed Processing](#distributed-processing)
    - [Code Optimization](#code-optimization)
    - [Configuration \& Validation](#configuration--validation)
    - [Optional Web Interface](#optional-web-interface)

---

## Design Philosophy

This pipeline is built to be modular, testable, and production-ready:

- **Modularity**: Core stages (ingestion, cleansing, transformation) are distinct Python modules in `src/` for reusability and clarity.
- **Testability**: Unit tests in `tests/` ensure component reliability.  
- **Production-Readiness**: Using virtual environments, dependency management, and a structure suitable for orchestration (via Airflow or Prefect).
- `src/main.py` orchestrates the full workflow.

---

## Project Structure
```
DRUG-DATA-PIPELINES/
├── .github/                  # CI/CD GitHub Actions
│   └── workflows/ci.yml
├── data/                     # Project Data
│   ├── raw/                  # Raw input data
│   ├── cleaned/              # Cleaned data
│   └── output/               # Results (JSON graph, adhoc analysis outputs)
├── sql/                      # SQL Queries (daily_sales.sql, sales_categorisation.sql)
├── src/                      
│   ├── analysis/             
│   │   └── adhoc_analysis.py # Ad-hoc analyses
│   ├── data_cleansing/       
│   │   └── date_parser.py    # Date standardization
│   ├── data_ingestion/       
│   │   └── reader.py         # Data loading
│   ├── data_transformation/  
│   │   └── drug_mention_finder.py # code finding drug mentions
│   ├── utils/                
│   │   └── utils.py          # Utility functions: Validators, text cleaning
│   └── main.py               # Pipeline entry point and orchestrator
├── tests/                    # Unit Tests
│   ├── test_utils.py         # an example, instead of testing everything (time concern)
│   └── sql/                  
│       └── test_sales_categorisation.py # Example SQL test
├── requirements.txt          # Python Dependencies
├── pyproject.toml            # Project configuration
├── .gitignore
├── LICENSE
└── README.md
```

## Data Sources

Raw input data is located in data/raw/:

- drugs.csv: atccode, drug
- pubmed.csv: id, title, journal, date
- pubmed.json: Same as pubmed.csv (JSON format)
- clinical_trials.csv: id, scientific_title, journal, date

## Installation

- Clone: `git clone https://github.com/Abdelhaq-Bensghir/drug-data-pipelines.git`
- cd drug-data-pipelines
- Create venv: `python3 -m venv .venv`
- Activate venv:
  - Bash on windows: `source .venv/Scripts/activate`
- Install dependencies: `pip install -r requirements.txt`
(Always activate venv before installing dependencies to ensure project isolation.)

## Pipeline Execution

Run the main pipeline: `python3 src/main.py`

### Key Steps:

- The pipeline loads raw data (merging PubMed csv and json sources).
- Cleans text fields (e.g., clean_skipped_hex_sequences).
- Validates ATC codes and NCT numbers.
- Standardizes date formats.
- Saves cleaned DataFrames to data/cleaned/.
- Finds drug mentions in titles.
- Generates data/output/drug_journal_mentions_graph.json.

## Output Structure

The main output data/output/drug_journal_mentions_graph.json is a flat JSON:

- drug (str): Drug name.
- journal (str): Journal name.
- date (str): Mention date (YYYY-MM-DD).
- source_type (str): "pubmed" or "clinical_trial".
- publication_id (str): Publication ID.
- publication_title (str): Publication title.

### Why a Flat JSON Format?

A flat list of mention events was chosen as an output because:
- Easier for Pandas, databases, and BI tools to process.
- Facilitates cross-cutting analyses (like the done adhoc analysis).
- Widely supported and easy to share/integrate, and it can be transformed into a nested structure if needed for specific use cases.

## Ad-hoc Analyses

Performs two analyses on the pipeline's JSON output:
- Journal mentioning the most different drugs.
- Drugs co-mentioned with a target drug in the same PubMed-only journals.

To execute it, run: `python3 src/analysis/adhoc_analysis.py`

## SQL Queries

They aren't part of the pipeline, but since it's within the same assignement pdf, and for simplicity of access, I added them in a sql/ dolder in the root of the project:
- daily_sales.sql
- sales_categorisation.sql

## Unit Tests

You can either :
- Run all tests: `python3 -m pytest`
- Run tests for a specific file:
  - `python3 -m pytest tests/test_utils.py`
  - or `python3 -m pytest tests/sql/sales_categorisation_test.py`
- Or run a specific test function:
  - `python3 -m pytest tests/test_utils.py::test_valid_atc_codes`
  - `python3 -m pytest tests/sql/sales_categorisation_test.py`
  - `python3 -m pytest tests/sql/sales_categorisation_test.py::test_sales_categorisation_query`

## Contribution

- Fork and create a feature branch.
- Develop and test.
- Format code (python -m black .).
- Open a Pull Request.

## License:
MIT License: See ./LICENSE file

## Scalability & Improvements

If I were to put this pipeline into production, I would have worked on some other aspects:

### Operational Enhancements

- **Logging & Monitoring**: We can add structured logging, to have logs to follow the execution of the scriots.
- **Containerization**: Package the pipeline using Docker to ensure consistent execution across environments.
- **Orchestration**: Break `main.py` into parameterizable, idempotent functions and use tools like **Airflow** or **Prefect** for task orchestration and scheduling.

### Data Format Upgrades

- **Scalable Storage**: For large datasets, we may consider switching from CSV/JSON to columnar formats like **Parquet** or **ORC**.
- **Compression & Partitioning**: We can use partitioning and clustering for datasets (for sql) queries.

### Distributed Processing

- **Large-Scale Processing**: Integrate distributed frameworks such as Apache Spark (via PySpark).

### Code Optimization

- **Vectorized Operations**: Replace `iterrows()` in the main.py for example with vectorized alternatives or `itertuples()` for better performance.
- **Function Refactoring**: Decompose large functions into testable, composable units to ease orchestration and testing.

### Configuration & Validation

- **Externalized Configs**: config files (`.ini`, `.yaml`, `.env`) to manage file paths, thresholds, and parameters.
- **Schema Validation**: Apply tools like **Pandera** to enforce data types, constraints, and schema validation throughout the pipeline.

### Web Interface

- Building a lightweight UI using tools like streamlit. Enabling exploration of JSON graphs and ad-hoc analysis results in a visual way.