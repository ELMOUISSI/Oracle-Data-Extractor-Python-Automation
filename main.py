import os
import pandas as pd
from sqlalchemy import create_engine
import warnings
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# --- 🔕 Ignore pandas warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# --- 📦 Load environment variables from .env
load_dotenv()

# --- 🔐 Oracle connection info
username = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST", "192.168.0.166")
port = os.getenv("ORACLE_PORT", "1521")
service_name = os.getenv("ORACLE_SERVICE", "TPXPROD")
MAX_THREADS = int(os.getenv("MAX_THREADS", 3))

# --- 📁 Folders
SQL_FOLDER = "sql"
OUTPUT_FOLDER = "roquete"

# --- ⚙️ Create Oracle DSN for Thick Client
dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service_name})))"
engine = create_engine(f"oracle+oracledb://{username}:{password}@{dsn}")

# --- 🧠 Execute a single SQL file
def execute_sql_file(file_path, engine):
    sql_name = os.path.splitext(os.path.basename(file_path))[0]
    output_base = os.path.join(OUTPUT_FOLDER, sql_name)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"\n--- 🚀 Starting extraction: {sql_name} ---")

    # Read SQL file safely
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_query = f.read()
    except UnicodeDecodeError:
        with open(file_path, "r", encoding="latin-1") as f:
            sql_query = f.read()

    print(f"✅ File loaded successfully: {sql_name}")

    try:
        # Connect
        start_conn = time.time()
        connection = engine.connect()
        end_conn = time.time()
        print(f"✅ Connected! (Time: {(end_conn - start_conn):.2f}s)\n")

        # Execute query
        start_extract = time.time()
        df = pd.read_sql(sql_query, connection)
        end_extract = time.time()
        total_rows = len(df)
        print(f"✅ {total_rows:,} row(s) fetched from Oracle. ⏱ {((end_extract - start_extract)/60):.2f} min\n")

        # Export result
        max_rows_excel = 1_048_000
        if total_rows > max_rows_excel:
            print("⚠️ Large dataset — splitting automatically...")
            chunks = [df[i:i + max_rows_excel] for i in range(0, total_rows, max_rows_excel)]
            for idx, chunk in enumerate(chunks, start=1):
                part_file = f"{output_base}_part_{idx}_{timestamp}.xlsx"
                chunk.to_excel(part_file, index=False)
                print(f"📁 Saved: {os.path.basename(part_file)} ({len(chunk):,} rows)")
        else:
            output_file = f"{output_base}_{timestamp}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📊 Result saved: {os.path.basename(output_file)} ({total_rows:,} rows)")

    except Exception as e:
        print(f"❌ Error while processing {sql_name}: {e}")

    finally:
        if 'connection' in locals():
            connection.close()
            print(f"🔒 Connection closed for {sql_name}.\n")

# --- 🧩 Main logic
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith(".txt") or f.endswith(".sql")]

if not sql_files:
    print("⚠️ No SQL files found in the 'sql' folder.")
else:
    print(f"🧵 Starting parallel execution with {MAX_THREADS} threads...\n")
    start_time = time.time()

    # Multithreading execution
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(execute_sql_file, os.path.join(SQL_FOLDER, f), engine): f for f in sql_files}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error in thread for {futures[future]}: {e}")

    end_time = time.time()
    total_min = (end_time - start_time) / 60
    print(f"\n✅ All SQL scripts processed successfully in {total_min:.2f} min.")
