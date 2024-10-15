import pandas as pd
from sqlalchemy import create_engine
import utilities  # Common utilities module
import data_cleaning  # Import for data cleaning

class DatabaseConnector:
    @staticmethod
    def read_db_creds(file_path):
        return utilities.read_db_creds(file_path)
    
    @staticmethod
    def init_db_engine(upload_creds):
        return utilities.init_db_engine(upload_creds)

    def upload_to_db(self, df, table_name, sales_path):
        creds = self.read_db_creds(sales_path)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USERNAME = creds.get('USERNAME') or creds.get('RDS_USER')
        PASSWORD = creds.get('PASSWORD') or creds.get('RDS_PASSWORD')
        HOST = creds.get('Host') or creds.get('RDS_HOST')
        PORT = creds.get('PORT') or creds.get('RDS_PORT')
        DATABASE = creds.get('DATABASE') or creds.get('RDS_DATABASE')
        upload_string = f"{DATABASE_TYPE}+{DBAPI}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        upload_engine = create_engine(upload_string)       
        df.to_sql(table_name, upload_engine, if_exists = 'replace', index=False)

if __name__ == "__main__":
    
    engine = utilities.engine    
    user_data_table = 'legacy_users'
    extractor = data_cleaning.data_extraction.DataExtractor()
    user_data_df = extractor.read_rds_table(engine, user_data_table)
    cleaned_df = data_cleaning.DataCleaning.clean_user_data(user_data_df)
    #print(cleaned_df)
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(cleaned_df, 'dim_users', 'sales_db_creds.yaml')
    print("Data uploaded successfully!")
