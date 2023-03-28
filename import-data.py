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
            website VARCHAR(255),
            rating FLOAT,
            rating_top INT,
            playtime INT,
            achievements_count INT,
            ratings_count INT,
            suggestions_count INT,
            game_series_count INT,
            reviews_count INT,
            platforms VARCHAR(255),
            developers VARCHAR(3000),
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
    reader = csv.DictReader(csvfile)
    total_rows = sum(1 for row in reader)
    csvfile.seek(0)  # Reset file pointer
    reader = csv.DictReader(csvfile)
    with tqdm(total=total_rows) as pbar:
        for row in reader:
            values = []
            for key, value in row.items():
                if value == '':
                    values.append('NULL')
                elif key == 'tba':
                    values.append(str(int(value.lower() == 'true')))
                else:
                    values.append("'" + value.replace("'", "''") + "'")
            sql = "INSERT INTO games (" + ', '.join(row.keys()) + ") VALUES (" + ', '.join(values) + ")"
            # print(sql)  # Debugging line
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()
            pbar.update(1)

# Close the connection
connection.close()

