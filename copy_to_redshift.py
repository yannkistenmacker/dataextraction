import boto3
import configparser
import psycopg2

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
schema = parser.get("aws_creds", "schema")

# Connect to the redshift cluster
rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port
)

# Load the account_id and iam_role from conf 
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# Run the COPY command to load the file into Redshift
file_path = ("s3://" + bucket_name + "/order_extract.csv")
role_string = ("arn:aws:iam::" + account_id + ":role" + iam_role)

sql = "COPY public.Orders"
sql = sql + " from %s "
sql = sql + " iam_role %s;"

# Create a cursor object and execute the COPY
cur = rs_conn.cursor()
cur.execute(sql,(file_path, role_string))

cur.close()
rs_conn.commit()

rs_conn.close()