
# CleanRide ETL Pipeline

A simple, modular ETL pipeline built in Python to extract taxi sales data from Parquet files and SQL databases, transform it with pandas, and load cleaned data into CSV files. The pipeline includes logging, monitoring, error handling, email alerting, unit tests, and can be scheduled using cron jobs.

---

## Features

- **Extract** data from multiple structured sources (Parquet and SQL)  
- **Transform** sales data by filtering, cleaning, and enriching with calculated fields  
- **Load** data into customized CSV files  
- **Robust Logging** for detailed pipeline run insights  
- **Monitoring & Validation** to ensure data quality at each step  
- **Exception Handling** with email alerts on failure  
- **Unit Testing** for core functions to ensure reliability  
- **Cron Job Scheduling** for automation  

---

## Project Structure

CleanRide_ETL_Pipeline/
├── data/                   # Raw data files (Parquet, SQLite DB)
├── extract/
│   ├── parquet_reader.py   # Read data from Parquet files
│   └── sql_reader.py       # Read data from SQL databases
├── transform/
│   └── sales_transformer.py # Clean & transform sales data
├── load/
│   └── csv_writer.py       # Save DataFrame to CSV file
├── utils/
│   ├── logger.py           # Custom logging setup
│   ├── monitor.py          # Data validation & monitoring helpers
│   └── alert.py            # Email alerting on failure
├── tests/                  # Unit tests for each module
├── main.py                 # Main ETL pipeline orchestration script
├── run_etl.sh              # Shell script to run pipeline (for cron)
└── README.md               # Project overview & instructions

---

## Prerequisites

- Python 3.7 or higher  
- `pandas`  
- `pytest` (for running tests)  
- SQLite3 (for SQL database if needed)  

Install dependencies with:

```bash
pip install pandas pytest
```

---

## How to Run

1. Place your raw data files inside the `data/` folder.  
2. Run the ETL pipeline:

```bash
python main.py
```

3. Check `output/cleaned_sales.csv` for the cleaned data output.  
4. View logs in the console or `etl.log` (if using cron).  

---

## Scheduling with Cron

- Use the included `run_etl.sh` script to schedule the pipeline in cron.  
- Edit the cron table with `crontab -e` and add:

```cron
0 * * * * /path/to/project/run_etl.sh
```

This runs the pipeline every hour.

---

## Email Alerts

- Configure your SMTP settings in `utils/alert.py` for alert emails on failure.  
- Update recipient email in `main.py`.

---

## Running Unit Tests

From the project root, run:

```bash
pytest tests/
```

---

## License

MIT License

---

## Contact

Created by Pankaj Singh — feel free to reach out for help or suggestions!