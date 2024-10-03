import numpy as np
import re
import pandas as pd 
from sqlalchemy import engine
import utilities # our shared utilities module
import data_extraction #Data extraction module
import database_utils
class DataCleaning: 
    @staticmethod
    def clean_user_data(user_data_df):
        
        df_to_clean = data_extraction.DataExtractor().read_rds_table(utilities.engine, user_data_df)
        dropped_rows = pd.DataFrame(df_to_clean).dropna(axis= 0, subset=['first_name', 'last_name'], how = 'any')
        dropped_rows = dropped_rows.dropna(axis= 1, how= 'any').drop_duplicates()
        dtformat = '%Y-%m-%d'
        dropped_rows['date_of_birth'] = pd.to_datetime(dropped_rows['date_of_birth'], errors='coerce', format=dtformat)
        filtered_country_codes = dropped_rows['country_code'].apply(lambda x: isinstance(x, str) and len(x) ==2 )        
        dropped_rows[dropped_rows['country_code'].isin(filtered_country_codes)].dropna()
        regex_expression = r'^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
        dropped_rows.loc[~dropped_rows['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan
        dropped_rows = dropped_rows.dropna()
        dropped_rows['address'] = dropped_rows['address'].apply(lambda x: ''.join(c for c in x if c.isalnum()))
        dropped_rows['email_address'] = dropped_rows['email_address'].apply(lambda x: x.strip() if isinstance(x,str) else np.nan)
        dropped_rows['email_address'].dropna()
        cleaned_df = dropped_rows
        return cleaned_df
    @staticmethod
    def clean_card_data():
        card_df =data_extraction.DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        dropped_na_card = card_df.dropna()
        dropped_duplicates = dropped_na_card.drop_duplicates()
        cc_regex = r'^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$'
        dropped_duplicates['card_number'] = dropped_duplicates['card_number'].apply(lambda x: x if re.match(cc_regex, str(x)) else np.nan)
        cleaned_card_df = dropped_duplicates.dropna(subset=['card_number'])
        return cleaned_card_df
    @staticmethod
    def clean_store_data(api_data):
        resp


if __name__ == "__main__":
   
    engine = utilities.engine  # INITIALIZE THE ENGINE USING UTILS MODULE
    user_data_table = 'legacy_users' # TABLE NAME TO EXTRACT USER DATA
    extractor = data_extraction.DataExtractor()# EXTRACTOR INSTANCE
    user_data_df = extractor.read_rds_table(engine, user_data_table)# READ DATA FROM THE SPECIFIED TABLE
    cleaned_df = DataCleaning.clean_user_data(user_data_df) # CLEAN THE EXTRACTED DATA
    cleaned_card_df = DataCleaning.clean_card_data()
    #print(cleaned_card_df)
    db_connector = database_utils.DatabaseConnector()
    db_connector.upload_to_db(cleaned_card_df, 'dim_card_details', 'sales_db_creds.yaml')
    print("Card details uploaded successfully!")
    
    









