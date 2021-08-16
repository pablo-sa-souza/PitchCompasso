import pandas as pd
import numpy as np
import pymysql
import os
import opendatasets as od
from io import BytesIO
from sqlalchemy import create_engine

#od.download("https://www.kaggle.com/ealaxi/banksim1/download")

#leitura dos df do CSV
df = pd.read_csv ("https://github.com/marcellocaron/desafio_pagseguro/raw/main/bs140513_032310.csv", sep =',', quotechar="'")

#busca valores distintos encontrados na coluna gender
unique_values_gender = df['gender'].unique()

#aplica na variavel off_genders os valores encontrados na variavel unique_values diferntes de M e F.
off_genders = unique_values_gender [(unique_values_gender != "M") & (unique_values_gender != "F")]

#Tira as aspas dos valores em off_gender
stripped_values_gender = [val.replace("'", "") for val in off_genders]

#cria variavel replace_regex que adiciona pipe entre os valores contidos em stripped values.
replace_regex_gender = "|".join(stripped_values_gender)

#coluna gender do dataframe modelo substiui os valores do off_genders por NaN
df['gender'] = df['gender'].replace(replace_regex_gender, '', regex=True)

#busca valores distintos encontrados na coluna age
unique_values_age = df['age'].unique()

#aplica na variavel age os valores encontrados na variavel unique_values diferntes de M e F.
off_age = unique_values_age [(unique_values_age == 'U')]

#Tira as aspas dos valores em off_age
stripped_values_age = [val.replace("'", "") for val in off_age]

#cria variavel replace_regex_age que adiciona pipe entre os valores contidos em stripped values.
replace_regex_age = "|".join(stripped_values_age)

#coluna gender do dataframe modelo substiui os valores do off_genders por NaN
df['age'] = df['age'].replace(replace_regex_gender, 0, regex=True)


#cria uma lista de condição para gerar a coluna month
conditionlist = [
    (df['step'] >= 0) &  (df["step"] <=30),
    (df["step"] >= 31) & (df["step"] <= 60),
    (df["step"] >= 61) & (df["step"] <= 90),
    (df["step"] >= 91) & (df["step"] <= 120),
    (df["step"] >= 121) & (df["step"] <= 150),
    (df["step"] >= 151) & (df["step"] <= 180)]
choicelist = [1, 2, 3, 4, 5, 6]
df['month'] = np.select(conditionlist, choicelist, default='Not Specified')

#cria coluna ano com valor de 2021
df.insert(loc=11, column='year', value=2021)

df['amount'][df['amount'] == 0.0] = None

ua = df['gender'].unique()

df['age'] = df['age'].astype('int32')

df['zipcodeOri'] = df['zipcodeOri'].astype('int32')

df['zipMerchant'] = df['zipMerchant'].astype('int32')

db_data = 'mysql+mysqldb://' + 'root' + ':' + 'root' + '@' + '172.21.48.1' + ':3306/' \
       + 'db' + '?charset=utf8mb4'
engine = create_engine(db_data)

db_data2 = 'mysql+mysqldb://' + 'root' + ':' + 'root' + '@' + '172.21.48.1' + ':3306/' + 'analytic'
engine = create_engine(db_data2)

connection = pymysql.connect(host='localhost',
                         user='root',
                         password='root',
                         db='db')

connection2 = pymysql.connect(host='localhost',
                         user='root',
                         password='root',
                         db='analytic')

cursor=connection.cursor()
# envia para o SQL os dados
df.to_sql('transactions', engine, if_exists='append', index=False)

try:
    cursor = connection2.cursor()
    cursor.callproc("sp_analytic_data")
    results = list(cursor.fetchall())
    cursor.close()
    connection.commit()
finally:
    connection.close()