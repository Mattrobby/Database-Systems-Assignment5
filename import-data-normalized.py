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

# Drop the child tables first
with connection.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS game_platforms")
    cursor.execute("DROP TABLE IF EXISTS game_developers")
    cursor.execute("DROP TABLE IF EXISTS game_genres")
    cursor.execute("DROP TABLE IF EXISTS game_publishers")

# Drop the parent tables
with connection.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS games")
    cursor.execute("DROP TABLE IF EXISTS platforms")
    cursor.execute("DROP TABLE IF EXISTS developers")
    cursor.execute("DROP TABLE IF EXISTS genres")
    cursor.execute("DROP TABLE IF EXISTS publishers")

# Create the tables
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
            esrb_rating VARCHAR(255),
            added_status_yet INT,
            added_status_owned INT,
            added_status_beaten INT,
            added_status_toplay INT,
            added_status_dropped INT,
            added_status_playing INT
        )CHARACTER SET utf8mb4 COLLATE 
    utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE platforms (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE developers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE genres (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE publishers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE game_platforms (
            game_id INT,
            platform_id INT,
            PRIMARY KEY (game_id, platform_id),
            FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
            FOREIGN KEY (platform_id) REFERENCES platforms(id) ON DELETE CASCADE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)
    
    cursor.execute("""
        CREATE TABLE game_developers (
            game_id INT,
            developer_id INT,
            PRIMARY KEY (game_id, developer_id),
            FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
            FOREIGN KEY (developer_id) REFERENCES developers(id) ON DELETE CASCADE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE game_genres (
            game_id INT,
            genre_id INT,
            PRIMARY KEY (game_id, genre_id),
            FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
            FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)

    cursor.execute("""
        CREATE TABLE game_publishers (
            game_id INT,
            publisher_id INT,
            PRIMARY KEY (game_id, publisher_id),
            FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
            FOREIGN KEY (publisher_id) REFERENCES publishers(id) ON DELETE CASCADE
        )CHARACTER SET utf8mb4 COLLATE
            utf8mb4_unicode_ci
    """)
    
def insert_or_get_id(table, name, cursor):
    cursor.execute(f"SELECT id FROM {table} WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result['id']
    else:
        cursor.execute(f"INSERT INTO {table} (name) VALUES (%s)", (name,))
        return cursor.lastrowid
def insert_or_get_id(table, name, cursor):
    cursor.execute(f"SELECT id FROM {table} WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result['id']
    else:
        cursor.execute(f"INSERT INTO {table} (name) VALUES (%s)", (name,))
        return cursor.lastrowid

def relationship_exists(table, game_id, related_id, related_column, cursor):
    cursor.execute(f"SELECT 1 FROM {table} WHERE game_id = %s AND {related_column} = %s", (game_id, related_id))
    return cursor.fetchone() is not None

# Insert data from the CSV file with progress bar
with open('game_info.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # Skip the header row
    total_rows = sum(1 for row in reader)
    csvfile.seek(0)  # Reset file pointer
    next(reader)  # Skip the header row again
    with tqdm(total=total_rows+1) as pbar:
        tba_index = header.index('tba')  # Get the index of 'tba' column
        platforms_col_index = header.index('platforms')
        developers_col_index = header.index('developers')
        genres_col_index = header.index('genres')
        publishers_col_index = header.index('publishers')
        for row in reader:
            row = [None if value == '' else value for value in row]

            # Convert 'True'/'False' string to 1/0 for the 'tba' column
            if row[tba_index].lower() == 'true':
                row[tba_index] = 1
            else:
                row[tba_index] = 0

            with connection.cursor() as cursor:
                platforms = row[platforms_col_index]
                if platforms is not None:
                    platforms = platforms.split('||')
                else:
                    platforms = []

                developers = row[developers_col_index]
                if developers is not None:
                    developers = developers.split('||')
                else:
                    developers = []

                genres = row[genres_col_index]
                if genres is not None:
                    genres = genres.split('||')
                else:
                    genres = []

                publishers = row[publishers_col_index]
                if publishers is not None:
                    publishers = publishers.split('||')
                else:
                    publishers = []

                game_values = [row[header.index(col)] for col in [
                    'id', 'slug', 'name', 'metacritic', 'released', 'tba', 'updated', 'website', 'rating',
                    'rating_top', 'playtime', 'achievements_count', 'ratings_count', 'suggestions_count',
                    'game_series_count', 'reviews_count', 'esrb_rating', 'added_status_yet', 'added_status_owned',
                    'added_status_beaten', 'added_status_toplay', 'added_status_dropped', 'added_status_playing'
                ]]

                cursor.execute("""
                    INSERT INTO games (id, slug, name, metacritic, released, tba, updated, website, rating, rating_top,
                        playtime, achievements_count, ratings_count, suggestions_count, game_series_count, reviews_count,
                        esrb_rating, added_status_yet, added_status_owned, added_status_beaten, added_status_toplay,
                        added_status_dropped, added_status_playing)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(game_values))

                game_id = row[header.index('id')]

                for platform in platforms:
                    platform_id = insert_or_get_id('platforms', platform, cursor)
                    if not relationship_exists("game_platforms", game_id, platform_id, "platform_id", cursor):
                        cursor.execute("INSERT INTO game_platforms (game_id, platform_id) VALUES (%s, %s)", (game_id, platform_id))

                for developer in developers:
                    developer_id = insert_or_get_id('developers', developer, cursor)
                    if not relationship_exists("game_developers", game_id, developer_id, "developer_id", cursor):
                        cursor.execute("INSERT INTO game_developers (game_id, developer_id) VALUES (%s, %s)", (game_id, developer_id))

                for genre in genres:
                    genre_id = insert_or_get_id('genres', genre, cursor)
                    if not relationship_exists("game_genres", game_id, genre_id, "genre_id", cursor):
                        cursor.execute("INSERT INTO game_genres (game_id, genre_id) VALUES (%s, %s)", (game_id, genre_id))

                for publisher in publishers:
                    publisher_id = insert_or_get_id('publishers', publisher, cursor)
                    if not relationship_exists("game_publishers", game_id, publisher_id, "publisher_id", cursor):
                        cursor.execute("INSERT INTO game_publishers (game_id, publisher_id) VALUES (%s, %s)", (game_id, publisher_id))

                connection.commit()
                pbar.update(1)

# Close the connection
connection.close()
                   

