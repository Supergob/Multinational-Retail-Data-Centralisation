from sqlalchemy import inspect
import pandas as pd
import utilities
import requests
import tabula
import boto3
from io import StringIO

class DataExtractor:
    @staticmethod
    def list_db_tables(engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    @staticmethod
    def list_number_of_stores(stores_endpoint, headers):
        response = requests.get(stores_endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return f"Error: {response.status_code}"
    @staticmethod
    def retrieve_stores_data(num_stores, retrieve_stores_endpoint, headers): 
        list_stores_returned =[]
        
        for store_number in range(0 ,num_stores):
            stores_returned = pd.DataFrame(list_stores_returned)
            store_url = f"{retrieve_stores_endpoint}/{store_number}"
            response = requests.get(store_url, headers=headers)
            
            if response.status_code == 200:
                store_data = response.json()
                list_stores_returned.append(store_data)
            
            else:
                print(f"failed to retrieve store{store_number}, Status code: {response.status_code}")
        
        if list_stores_returned:
            stores_returned = pd.DataFrame(list_stores_returned)
            return stores_returned
        else:
            return "Error, Empty DataFrame"
    
    @staticmethod         
    def read_rds_table(engine, table_name):
        query = f"SELECT * FROM {table_name}"
        read_data = pd.read_sql_query(query, engine)
        return read_data
    
    @staticmethod
    def retrieve_pdf_data(link):
        data_to_convert =  tabula.read_pdf(link, pages = 'all')
        if isinstance(data_to_convert, list) and all(isinstance(df, pd.DataFrame) for df in data_to_convert):
            data_to_convert = pd.concat(data_to_convert, ignore_index= True)
        else:
            raise ValueError('Not everything in here is a list')
        return data_to_convert

    @staticmethod
    def extract_from_s3(address):
        s3 = boto3.client('s3')
        target_address = s3.get_object(Bucket='data-handling-public', Key='products.csv')
        raw_csv = s3_data = target_address['Body'].read().decode('utf-8')
        extracted_data = pd.read_csv(StringIO(raw_csv))
        return extracted_data
    
    @staticmethod
    def extract_from_s3_json(json_address):
        s3 = boto3.client('s3')
        target_address = s3.get_object(Bucket='data-handling-public', Key='date_details.json')
        raw_json = s3_data = target_address['Body'].read().decode('utf-8')
        extracted_data = pd.read_json(StringIO(raw_json))
        return extracted_data


if __name__== "__main__":
    engine = utilities.engine         # Used to call engine
    user_data_table = 'orders_table'  # Replace with the actual table name for the table you need to retrieve data from
    extractor = DataExtractor()
    user_data_df = extractor.read_rds_table(engine, user_data_table)
    Retrive_pdf = DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    retrieve_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    number_of_stores = 450
    address = 's3://data-handling-public/products.csv'
    addresss = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
 