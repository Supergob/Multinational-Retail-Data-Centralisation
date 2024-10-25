import data_extraction          # Data extraction module
import database_utils           # Contains methods for interacting with the database
import numpy as np              # Numpy library
import pandas as pd             # Pandas library
import re                       # Regex
from sqlalchemy import engine   # SQL Alchemy engine
import utilities                # Helper methods like reading database credentials shared between scripts

class DataCleaning: 
    @staticmethod
    def clean_user_data(user_data_df):
        raw_data = extractor.read_rds_table(engine, 'legacy_users')
        raw_data.replace('NULL', np.nan, inplace=True)
        raw_data.dropna(inplace=True)
        raw_data['join_date'] = pd.to_datetime(raw_data['join_date'], errors='coerce', format='mixed')
        raw_data.dropna(inplace=True)
        cleaned_user_data = raw_data
        return cleaned_user_data

    
    @staticmethod
    def clean_card_data(link): 
        raw_data = extractor.retrieve_pdf_data(link)    
        raw_data = raw_data.replace('NULL', np.nan)
        raw_data = raw_data.dropna()
        raw_data.drop_duplicates(subset='card_number', inplace=True)                                                                    # Remove duplicate card numbers
        raw_data['card_number'] = raw_data['card_number'].apply(lambda x: re.sub(r'\D', '', str(x)))                                    # Use regex to remove all non-numeric characters from card_number
        raw_data = raw_data[raw_data['card_number'].apply(lambda x: x.isdigit() and len(x) > 0)]                                        # Remove rows where card_number is now empty or invalid after the cleanup
        raw_data['date_payment_confirmed'] = pd.to_datetime(raw_data['date_payment_confirmed'], errors='coerce', format = 'mixed')      # Convert date_payment_confirmed to datetime
        raw_data.dropna(subset=['date_payment_confirmed'], inplace=True)                                                                # Remove rows where date_payment_confirmed is NaN
        cleaned_card_df = raw_data
        return cleaned_card_df



    
    @staticmethod
    def clean_store_data(num_stores, retrieve_stores_endpoint, headers):
        raw_data =extractor.retrieve_stores_data(num_stores, retrieve_stores_endpoint, headers)   # Pull the data 
        raw_data = raw_data.replace('NULL', np.nan)
        raw_data.dropna()
        raw_data['opening_date'] = pd.to_datetime(raw_data['opening_date'], errors= 'coerce')
        raw_data = raw_data.dropna(subset=['opening_date'])
        pattern = r'[^0-9]'
        raw_data['staff_numbers'] = raw_data['staff_numbers'].apply(lambda x:re.sub(pattern,'',str(x)))
        raw_data['staff_numbers'] = raw_data['staff_numbers'].replace('',np.nan)
        raw_data['staff_numbers'] = raw_data['staff_numbers'].dropna()
        cleaned_store_df = raw_data
        return cleaned_store_df
    
    @staticmethod
    def convert_product_weights(extracted_s3_data):
        weights = extracted_s3_data['weight']
        weights_in_kg = []
        for i in weights:
            if isinstance(i, (float, int)):              # If the value is a float, assume it's already in kg
                weights_in_kg.append(f"{i}kg")
            elif isinstance(i, (str)):                   # If the value is a string, process it                 
                match = re.findall(r'\d+\.?\d*', i)      # Clean up the string to extract the numeric value
                if match:
                    value_of_weights = float(match[0])  
                if 'kg' in i:
                    weights_in_kg.append(f"{value_of_weights}kg")
                elif 'g' in i:
                    weights_in_kg.append(f"{value_of_weights / 1000}kg")
                elif 'ml' in i:
                    weights_in_kg.append(f"{value_of_weights / 1000}kg")
                elif 'oz' in i:
                    weights_in_kg.append(f"{value_of_weights * 0.0283495}kg")
                else:
                    weights_in_kg.append(np.nan)
            else:
                weights_in_kg.append(np.nan)     
    
        extracted_s3_data['weight'] = weights_in_kg
        extracted_s3_data.dropna(subset = ['weight'], inplace = True)
        extracted_s3_data.drop('Unnamed: 0', axis = 1, inplace = True)
        cleaned_product_weights = extracted_s3_data
        
        return cleaned_product_weights
          
    @staticmethod
    def clean_products_data(extracted_s3_data):
        raw_data = DataCleaning.convert_product_weights(extracted_s3_data)
        raw_data = raw_data.replace('NULL',np.nan)
        raw_data.dropna(inplace=True)
        cleaned_products_data = raw_data
        return cleaned_products_data
                                                                                                                                            
    @staticmethod
    def clean_orders_data(orders_data):
        raw_data = orders_data.drop(['first_name','last_name','1','level_0'], axis = 1)
        cleaned_orders_data = raw_data
        return cleaned_orders_data
        
    @staticmethod
    def clean_date_times(date_data):
        
        date_data.drop_duplicates(inplace = True)
        month_check = r'^(0?[1-9]|1[0-2])$'
        date_data['month'] = date_data['month'].apply(lambda x: x if re.match(month_check, str(x)) else np.nan )
        date_data = date_data.dropna(subset = ['month'])
        cleaned_data_data = date_data
        return cleaned_data_data
        
        
if __name__ == "__main__":
   
    engine = utilities.engine                                                       # Calls the engine                                                               
    extractor = data_extraction.DataExtractor()   
       
    # Upload the cleaned user details
    
    # user_data_table = 'legacy_users'
    # user_data_df = extractor.read_rds_table(engine, user_data_table)    
    # cleaned__user_df = DataCleaning.clean_user_data(user_data_df)                                                                               
    # db_connector = database_utils.DatabaseConnector()        
    # db_connector.upload_to_db(cleaned__user_df, 'dim_users','sales_db_creds.yaml')
    # #print(DataCleaning.clean_user_data(user_data_df))
    # print("Cleaned user data uploaded successfully!")
    
    # # Upload the cleaned card data
    
    # link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # cleaned_card_df = DataCleaning.clean_card_data(link)
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_card_df, 'dim_card_details', 'sales_db_creds.yaml')
    # #print(DataCleaning.clean_card_data(link))
    # print('Cleaned card details uploaded succesfully!')
    
    # # Upload the cleaned store data

    # headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    # number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # retrieve_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    # number_of_stores = 451
    # cleaned_store_df = DataCleaning.clean_store_data(number_of_stores, retrieve_stores_endpoint, headers)         
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_store_df, 'dim_store_details','sales_db_creds.yaml')
    # #print(DataCleaning.clean_store_data(number_of_stores, retrieve_stores_endpoint, headers))
    # print('Cleaned store details uploaded succesfully!')

    # # Upload the cleaned products data
    
    address = 's3://data-handling-public/products.csv'
    extracted_s3_data = data_extraction.DataExtractor.extract_from_s3(address)
    cleaned_product_data = DataCleaning.clean_products_data(extracted_s3_data)
    db_connector = database_utils.DatabaseConnector()
    db_connector.upload_to_db(cleaned_product_data, 'dim_products','sales_db_creds.yaml')
    #print(DataCleaning.clean_products_data(extracted_s3_data))
    print('Cleaned product details uploaded succesfully')
    
    # Upload the cleaned Orders table data
    
    # orders_table_name = 'orders_table'
    # orders_data = extractor.read_rds_table(utilities.engine, orders_table_name)
    # cleaned_orders_table = DataCleaning.clean_orders_data(orders_data)
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_product_data, 'dim_products','sales_db_creds.yaml')
    # print('Cleaned product details uploaded succesfully')
    
    # Upload the cleaned Orders table data
    
    # orders_table_name = 'orders_table'
    # orders_data = extractor.read_rds_table(utilities.engine, orders_table_name)
    # cleaned_orders_table = DataCleaning.clean_orders_data(orders_data)
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_orders_table,'orders_table','sales_db_creds.yaml')
    # print('Orders table uploaded succesfully!')
    
    # Upload the cleaned sales data

    # date_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    # date_data = extractor.extract_from_s3_json(date_address)
    # cleaned_date_data = DataCleaning.clean_date_times(date_data)
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_date_data, 'dim_date_times', 'sales_db_creds.yaml')
    # print('Cleaned sales data uploaded succesfully')