import pandas as pd
import os
import logging
from sqlalchemy import create_engine
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)    

engine = create_engine('sqlite:///inventory.db', connect_args={'check_same_thread': False})

def ingest_db(df, table_name, engine):
    '''this functionn will ingest the dataframe into db table'''
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    
def load_row_data():
    '''This fuction load cvs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_start = time.time()
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
            file_end = time.time()
            file_time = (file_end - file_start)
            logging.info(f'{file} loaded in {file_time:.2f} seconds')
    end = time.time()
    total_time = (end - start)/60
    logging.info('Ingestion Complete')
    logging.info(f'Total Time Taken: {total_time:.2f} Minutes')

if __name__ == "__main__":
    load_row_data()
