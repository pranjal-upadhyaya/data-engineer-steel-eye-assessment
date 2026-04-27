# SteelEye Data Engineer Assessment

A Python pipeline that downloads financial instrument data from the ESMA FIRDS API, parses a large XML file in memory-efficient batches, and writes the output to CSV — with optional upload to AWS S3 or Azure Blob Storage.

---

## Repository Structure

```
├── app/
│   ├── config.py          # Application configuration (pydantic-settings)
│   ├── model.py           # Pydantic model for ESMA API file records
│   ├── pipeline.py        # Orchestrator: ties fetcher and parser together
│   ├── xml_fetcher.py     # Downloads metadata and XML zip files from ESMA API
│   ├── xml_parser.py      # Streams and parses XML, writes CSV, uploads to cloud
│   └── utils/
│       └── logging.py     # Loguru logger initialisation
├── test/
│   ├── test_xml_fetcher.py    # Unit tests for XMLFetcher
│   ├── test_xml_parser.py     # Unit tests for XMLParser
│   └── test_integration.py    # Integration tests for the full pipeline
├── .github/
│   └── workflows/
│       └── tests.yml      # CI: runs pytest on push to dev
├── .pre-commit-config.yaml
├── .env                   # Local environment configuration (not committed)
├── main.py                # Entry point
└── pyproject.toml
```

---

## Pipeline Overview

1. Fetch file metadata from the ESMA FIRDS Solr API.
2. Select the target `DLTINS` file by index from the response (defaults to the second entry, configurable via `file_index`).
3. Download and extract the zip file.
4. Stream-parse the XML using `lxml.iterparse` in batches of 1 000 records.
5. Write a CSV with the following columns:

   | Column | Description |
   |---|---|
   | `FinInstrmGnlAttrbts.Id` | Instrument identifier |
   | `FinInstrmGnlAttrbts.FullNm` | Full name |
   | `FinInstrmGnlAttrbts.ClssfctnTp` | Classification type |
   | `FinInstrmGnlAttrbts.CmmdtyDerivInd` | Commodity derivative indicator |
   | `FinInstrmGnlAttrbts.NtnlCcy` | Notional currency |
   | `Issr` | Issuer |
   | `a_count` | Number of lowercase `a` characters in `FullNm` |
   | `contains_a` | `YES` if `a_count > 0`, `NO` otherwise |

6. Delete the XML file to free local storage.
7. Optionally upload the CSV to cloud storage (S3 or Azure).

---

## Setup

### Prerequisites

- Python 3.13
- [Poetry](https://python-poetry.org/docs/#installation)

### Install dependencies

```bash
poetry install
```

### Configure environment

Copy the example below into a `.env` file at the project root and fill in the values:

```env
BASE_FOLDER="temp"

# Leave empty to keep the CSV locally.
# Set to an fsspec URL to upload to cloud after processing.
# AWS S3:  CSV_STORAGE_PATH="s3://my-bucket/csv"
# Azure:   CSV_STORAGE_PATH="abfs://container@account.dfs.core.windows.net/csv"
CSV_STORAGE_PATH=""

# AWS credentials (used automatically by s3fs when CSV_STORAGE_PATH is s3://)
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""

# Azure credentials (used automatically by adlfs when CSV_STORAGE_PATH is abfs://)
AZURE_STORAGE_ACCOUNT_NAME=""
AZURE_STORAGE_ACCOUNT_KEY=""
```

### Run the pipeline

```bash
poetry run python main.py
```

Output files are written to:

```
temp/
├── xml/    # Downloaded XML files (deleted after parsing)
└── csv/    # Generated CSV files
```

---

## Testing

```bash
# Run all tests
poetry run pytest test/ -v

# Run only unit tests
poetry run pytest test/test_xml_fetcher.py test/test_xml_parser.py -v

# Run only integration tests
poetry run pytest test/test_integration.py -v
```

---

## Linting

Pre-commit hooks run `ruff check` and `ruff format` automatically on each commit.

To install the hooks:

```bash
poetry run pre-commit install
```

To run manually:

```bash
poetry run ruff check --fix .
poetry run ruff format .
```

---

## CI

GitHub Actions runs the full test suite on every push to the `dev` branch. See [`.github/workflows/tests.yml`](.github/workflows/tests.yml).
