import psycopg2
import csv
import os

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="admin",
    password="admin",
    port=5432
)
cursor = conn.cursor()

# CSV files and their corresponding tables
csv_files = {
    'activity_types.csv': 'activity_types',
    'activity.csv': 'activity',
    'deal_changes.csv': 'deal_changes',
    'fields.csv': 'fields',
    'stages.csv': 'stages',
    'users.csv': 'users'
}

raw_data_dir = './raw_data'

for csv_file, table_name in csv_files.items():
    filepath = os.path.join(raw_data_dir, csv_file)
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
            conn.commit()
            print(f"✓ Loaded {csv_file} into {table_name}")
    else:
        print(f"✗ File not found: {filepath}")

cursor.close()
conn.close()
print("\nData loading completed!")
