from sqlalchemy import create_engine
import yaml
import requests
def read_db_creds(file_path):
    with open(file_path,'r') as f:
        return yaml.safe_load(f)  
    
def init_db_engine(file_path):
       
        creds = read_db_creds(file_path)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = creds.get('RDS_USER')
        PASSWORD = creds.get('RDS_PASSWORD')
        HOST = creds.get('RDS_HOST')
        PORT = creds.get('RDS_PORT')
        DATABASE = creds.get('RDS_DATABASE')
        connection_string = (f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return create_engine(connection_string)



file_path = 'db_creds.yaml'
sales_path = 'sales_db_creds.yaml'
engine = init_db_engine('db_creds.yaml')

db_creds = read_db_creds(file_path)
#print(f"DB Credentials from {db_creds_path}: {db_creds}")

sales_creds = read_db_creds(sales_path)
#print(f"Sales DB Credentials from {sales_path}: {sales_creds}")