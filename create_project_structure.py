import os

# Define project root directory
project_root = "CleanRide_ETL_Pipeline"

# Define directory structure
dirs = [
    "data",
    "extract",
    "transform",
    "load",
    "utils",
    "tests",
]

# Define files to create with their relative paths
files = [
    "extract/parquet_reader.py",
    "extract/sql_reader.py",
    "transform/sales_transformer.py",
    "load/csv_writer.py",
    "utils/logger.py",
    "utils/monitor.py",
    "utils/alert.py",
    "tests/test_parquet_reader.py",
    "tests/test_sql_reader.py",
    "tests/test_sales_transformer.py",
    "tests/test_csv_writer.py",
    "main.py",
    "run_etl.sh",
    "README.md"
]

# Create directories
for d in dirs:
    dir_path = os.path.join(project_root, d)
    os.makedirs(dir_path, exist_ok=True)
    print(f"Created directory: {dir_path}")

# Create empty files
for f in files:
    file_path = os.path.join(project_root, f)
    # Ensure parent directory exists (in case)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Create empty file
    with open(file_path, "w") as file:
        file.write("")  # empty file
    print(f"Created file: {file_path}")

print("Project structure created successfully.")
