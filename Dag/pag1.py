import datetime
import logging
import time

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


def _get_data(url: str, ti):
    logging.info("GET-DATA STARTED")

    logging.info(f'GET-DATA Starting download')
    start = time.time()
    df = pd.read_csv(url, sep=',', quotechar='\'')
    end = time.time()
    logging.info(f'GET-DATA Finished downloading data in {end - start}')

    logging.info(f'GET-DATA Saving data to dictionary')
    dict_df = df.to_dict()

    logging.info(f'GET-DATA Sending data through queue')
    ti.xcom_push(key='raw_dataset', value=dict_df)

    logging.info(f'GET-DATA FINISHED')


def _process_data(ti):
    logging.info('PROCESS-DATA STARTED')
  
    logging.info('PROCESS-DATA Getting raw data from queue')
    json_df = ti.xcom_pull(key='raw_dataset', task_ids=['get_data'])

    logging.info(type(json_df[0]))
    
    logging.info('PROCESS-DATA Transforming raw data to dataframe')
    df = pd.DataFrame.from_dict(json_df[0], orient='columns')

    logging.info('PROCESS-DATA Removing unwanted values from "gender" column')
    # Busca valores distintos encontrados na coluna gender
    unique_values_gender = df['gender'].unique()
    

    # Aplica na variavel off_genders os valores encontrados na variavel unique_values diferntes de M e F.
    off_genders = unique_values_gender[(unique_values_gender != 'M') & (unique_values_gender != 'F')]

    # Tira as aspas dos valores em off_gender
    stripped_values_gender = [val.replace('\'', '') for val in off_genders]

    # Cria variavel replace_regex que adiciona pipe entre os valores contidos em stripped values.
    replace_regex_gender = '|'.join(stripped_values_gender)

    # Coluna gender do dataframe modelo substiui os valores do off_genders por NaN
    df['gender'] = df['gender'].replace(replace_regex_gender, '', regex=True)

    logging.info('PROCESS-DATA Removing unwanted values from "age" column')
    # Busca valores distintos encontrados na coluna age
    unique_values_age = df['age'].unique()

    # Aplica na variavel age os valores encontrados na variavel unique_values diferntes de M e F.
    off_age = unique_values_age[(unique_values_age == 'U')]

    # Tira as aspas dos valores em off_age
    stripped_values_age = [val.replace('\'', '') for val in off_age]

    # Cria variavel replace_regex_age que adiciona pipe entre os valores contidos em stripped values.
    replace_regex_age = '|'.join(stripped_values_age)

    # Coluna gender do dataframe modelo substiui os valores do off_genders por NaN
    df['age'] = df['age'].replace(replace_regex_age, 0, regex=True)

    logging.info('PROCESS-DATA Generating new column "month"')
    # Cria uma lista de condição para gerar a coluna month
    conditionlist = [
        (df['step'] >= 0) & (df['step'] <= 30),
        (df['step'] >= 31) & (df['step'] <= 60),
        (df['step'] >= 61) & (df['step'] <= 90),
        (df['step'] >= 91) & (df['step'] <= 120),
        (df['step'] >= 121) & (df['step'] <= 150),
        (df['step'] >= 151) & (df['step'] <= 180)]
    choicelist = [1, 2, 3, 4, 5, 6]
    df['month'] = np.select(conditionlist, choicelist, default='Not Specified')

    logging.info('PROCESS-DATA Generating new column "year"')
    # Cria coluna ano com valor de 2021
    df.insert(loc=11, column='year', value=2021)

    logging.info('PROCESS-DATA Replacing zero values with None')
    df['amount'][df['amount'] == 0.0] = None

    logging.info('PROCESS-DATA Ensuring column types')
    df['age'] = df['age'].astype('int32')
    df['zipcodeOri'] = df['zipcodeOri'].astype('int32')
    df['zipMerchant'] = df['zipMerchant'].astype('int32')

    logging.info(f'PROCESS-DATA Saving data to dictionary')
    dict_df = df.to_dict()

    logging.info('PROCESS-DATA Sending formatted data through queue')
    ti.xcom_push(key='processed_dataset', value=dict_df)

    logging.info('PROCESS-DATA FINISHED')


def _load_data(ti):
    logging.info('LOAD-DATA STARTED')

    db_data = 'mysql+mysqldb://' + 'root' + ':' + 'alcool666' + '@' + '172.28.64.1' + ':3306/' + 'db' + '?charset=utf8mb4'
    engine = create_engine(db_data)

    logging.info('LOAD-DATA Getting formatted data from queue')
    json_df = ti.xcom_pull(key='processed_dataset', task_ids=['process_data'])[0]

    logging.info('LOAD-DATA Transforming formatted data to dataframe')
    df = pd.DataFrame.from_dict(json_df, orient='index')

    # Envia para o SQL os dados
    logging.info('LOAD-DATA Saving formatted data to MySQL database')
    start = time.time()
    df.to_sql('transactions', engine, if_exists='append', index=False)
    end = time.time()
    logging.info(f'LOAD-DATA Data saved to MySQL in {end - start}')

    logging.info('LOAD-DATA FINISHED')

url = 'https://github.com/marcellocaron/desafio_pagseguro/raw/main/bs140513_032310.csv'


with DAG(dag_id='dag_pag_B', start_date=datetime.datetime(2020, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    get_data_task = PythonOperator(
        task_id='get_data',
        execution_timeout=datetime.timedelta(seconds=360),
        python_callable=_get_data,
        op_kwargs=dict(url='https://github.com/marcellocaron/desafio_pagseguro/raw/main/bs140513_032310.csv')
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        execution_timeout=datetime.timedelta(seconds=360),
        python_callable=_process_data
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        execution_timeout=datetime.timedelta(seconds=360),
        python_callable=_load_data
    )

    get_data_task >> process_data_task >> load_data_task