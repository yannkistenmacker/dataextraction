import pymysql
import csv
import boto3
import configparser
import psycopg2

# MySQL credentials
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

# Connect to MySQL database
conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

# Check if connection to MySQL database is succesfull
if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established")

# Query data from MySQL Database
m_query = "SELECT * FROM Orders;"
local_filename = "order_extract.csv"
m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

# Write query results to a local CSV file
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)
    fp.close()
    m_cursor.close()
    conn.close()

# Connection with AWS S3
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# Upload the local CSV file to S3 Bucket
try:
    s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)
    s3_file = local_filename
    s3.upload_file(local_filename, bucket_name, s3_file)
    print("File successfully uploaded to S3 bucket", bucket_name)
except Exception as e:
    print("Error uploading file to S3", e)

# Get db Redshift connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds" "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# Connect to the redshift cluster
rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port)

rs_sql = """SELECT COALESCE(MAX(LastUpdated), '1900-01-01) FROM Orders;"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

# There is only one row and column returned
last_updated_warehouse = result[0]
rs_cursor.close()
rs_conn.commit()