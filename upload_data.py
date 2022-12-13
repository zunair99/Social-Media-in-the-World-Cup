# Import the necessary libraries
import psycopg2 as ps
from twitter_data import auth, get_tweets, transform_df
import os

# Import DataFrame from twitter_data.py
df = transform_df()
print(df)
df.dtypes

# Define the RDS database connection parameters
host_name = os.environ.get("RDS_HOSTNAME")
database_name = os.environ.get("RDS_DB_NAME")
port = os.environ.get("RDS_PORT")
username = os.environ.get("RDS_USERNAME")
password = os.environ.get("RDS_PASSWORD")

# Define a function to connect to the RDS database
def connect_to_rds(host_name, database_name, port, username, password):
    try:
        conn = ps.connect(host=host_name, database=database_name, port=port, user=username, password=password)
    except ps.OperationalError as e:
        print("Error: Could not connect to RDS Database.")
        raise e
    else:
        print("Connected to RDS Database.")
    return conn

# Connect to the RDS database
conn = connect_to_rds(host_name, database_name, port, username, password)
print(df)
def create_table(curr):
    # Create a table to store the tweets and their metadata
    create_table_comm = (
        """
        CREATE TABLE IF NOT EXISTS tweets (
            created_at DATETIME,
            id VARCHAR(255),
            source VARCHAR(255),
            text VARCHAR(255),
            extraction_date DATETIME,
            media_check VARCHAR(255),
            poll_check VARCHAR(255),
            retweet_count INT,
            reply_count INT,
            like_count INT,
            quote_count INT,
            edit_history VARCHAR(255)
        )
        """
    )

    # Execute the SQL command to create the table
    curr.execute(create_table_comm)

# Create a cursor object to execute SQL commands on the RDS database connection object (conn)
curr = conn.cursor()

# Create the table
create_table(curr)

# Commit the changes to the database
conn.commit()

# Check if the table was created
curr.execute("SELECT * FROM tweets")
print(curr.fetchall())

# Define a function to insert the DataFrame into the RDS database
def insert_df(df, curr):
    # Create a list of tuples from the DataFrame values
    tuples = [tuple(x) for x in df.to_numpy()]

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO tweets (%s) VALUES %%s" % cols

    # Execute the SQL command to insert the DataFrame
    curr.execute(query, tuples)

    return curr.rowcount()