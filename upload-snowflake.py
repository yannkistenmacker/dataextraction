import configparser
import snowflake.connector

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("snowflake_creds", "username")
password = parser.get("snowflake_creds", "password")
account_name = parser.get("snowflake_creds", "account_name")
database = parser.get("snowflake_creds", "database")
schema = parser.get("snowflake_creds", "schema")

snow_conn = snowflake.connector.connect(
    user = username,
    password = password,
    account = account_name,
    database = database,
    schema = schema
)

sql = """COPY INTO ORDERS
FROM @MY_S3_STAGE
ON_ERROR = 'skip_file'"""

cur = snow_conn.cursor()
try:
    cur.execute(sql)
    print("Query executada com sucesso!")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Erro ao executar a query: {e}")
cur.close()
