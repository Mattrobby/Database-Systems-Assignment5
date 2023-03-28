import os
import csv
import pymysql.cursors
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Database credentials
host = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
db = os.getenv('DB_NAME')

# Connect to the database
connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db,
    cursorclass=pymysql.cursors.DictCursor
)

# Drop the table if it exists
with connection.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS games")

# Create the table
with connection.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE games (
            id INT PRIMARY KEY,
            slug VARCHAR(255),
            name VARCHAR(255),
            metacritic VARCHAR(255),
            released DATE,
            tba BOOLEAN,
            updated DATETIME,
            website VARCHAR(500),
            rating FLOAT,
            rating_top INT,
            playtime INT,
            achievements_count INT,
            ratings_count INT,
            suggestions_count INT,
            game_series_count INT,
            reviews_count INT,
            platforms VARCHAR(255),
            developers VARCHAR(2000),
            genres VARCHAR(255),
            publishers VARCHAR(255),
            esrb_rating VARCHAR(255),
            added_status_yet INT,
            added_status_owned INT,
            added_status_beaten INT,
            added_status_toplay INT,
            added_status_dropped INT,
            added_status_playing INT
        )
    """)

# Insert data from the CSV file with progress bar
with open('game_info.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Skip the header row
    total_rows = sum(1 for row in reader)
    csvfile.seek(0)  # Reset file pointer
    next(reader)  # Skip the header row again
    with tqdm(total=total_rows) as pbar:
        tba_index = header.index('tba')  # Get the index of 'tba' column

        for row in reader:
            row = [None if value == '' else value for value in row]

            # Convert 'True'/'False' string to 1/0 for the 'tba' column
            if row[tba_index].lower() == 'true':
                row[tba_index] = 1
            else:
                row[tba_index] = 0

            sql = "INSERT INTO games (" + ', '.join(header) + ") VALUES (" + ', '.join(['%s'] * len(header)) + ")"
            with connection.cursor() as cursor:
                # print(f"SQL Query: {sql}\nRow: {row}\nNumber of placeholders: {sql.count('%s')}, Number of values: {len(row)}\n")
                cursor.execute(sql, tuple(row))

            connection.commit()
            pbar.update(1)

# Close the connection
connection.close()
