import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import os

# Use service name 'postgres' for the database URL
URL = "postgresql://alt_school_user:secretPassw0rd@db:5432/ecommerce"

extract_dir = "/data"

engine = create_engine(URL)

if not os.path.exists(extract_dir):
    print(f"Directory {extract_dir} does not exist.")
else:
    print(f"Directory {extract_dir} exists. Loading CSV files...")

    files = os.listdir(extract_dir)
    if not files:
        print(f"No files found in {extract_dir}.")
    else:
        print(f"Files in {extract_dir}: {files}")

for csv_file in files:
    if csv_file.endswith(".csv"):
        file_path = os.path.join(extract_dir, csv_file)

        print(f"Processing file: {file_path}")

        try:
            df = pd.read_csv(file_path)
            print(f"Read {len(df)} rows from {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
            continue

        table_name = os.path.splitext(csv_file)[0]

        try:
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"Data from {csv_file} has been loaded into table {table_name}.")
        except Exception as e:
            print(f"Error writing {table_name} to database: {str(e)}")

print("All CSV files have been processed and loaded into the PostgreSQL database.")
