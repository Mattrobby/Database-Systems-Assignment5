import os
import random
import pymysql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database credentials
host = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_NAME')

# Connect to the MariaDB database
connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Create the 'items' table
with connection.cursor() as cursor:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            column1 INT,
            column2 FLOAT
        )
    ''')
    connection.commit()

# Insert 10 random entries into the 'items' table
with connection.cursor() as cursor:
    for _ in range(10):
        random_column1 = random.randint(1, 100)
        random_column2 = random.uniform(1, 100)
        cursor.execute(
            'INSERT INTO items (column1, column2) VALUES (%s, %s)',
            (random_column1, random_column2)
        )
    connection.commit()

# Close the connection
connection.close()


