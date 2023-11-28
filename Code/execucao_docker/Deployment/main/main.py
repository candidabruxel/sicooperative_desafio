import logging
import importlib.util
from datetime import datetime
import os
import pandas as pd
import warnings
import main.functions as func
warnings.filterwarnings('ignore')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
from sqlalchemy import create_engine

# Definição dos caminhos e sistema
if os.name == 'nt':
    logging.info('Executando no Windows...')
    path_func = os.getcwd()+'\\func\\'
    path_proc =  os.getcwd()+'\\proc\\'
    path_local =  os.getcwd()+'\\'
    hoje = datetime.now()

elif os.name == 'posix':
    logging.info('Executando no Linux...')
    path_func = '/app/base-de-laudos/Code/Deployment/main/func/'
    path_proc = '/app/base-de-laudos/Code/Deployment/main/proc/'
    path_local = '/app/base-de-laudos/Code/Deployment/main/'
    hoje = pd.to_datetime(os.getenv('datetime.now()')) 

else:
    logging.error('Não foi possivel identificar o sistema.')
    raise



func = func.module_from_file("func", path_func + "__init__.py")   
proc = func.module_from_file("proc", path_proc + "__init__.py")

from func import executaProc
from func import ajustesFinaisDf
from func import get_data_types
from func import sqlCodes

DB_USERNAME = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_DATABASE = 'desafio'

# Conexão usando SQLAlchemy
string_conexao = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# Crie uma engine usando a string de conexão
engine = create_engine(string_conexao)

# Configuração do Spark com o driver JDBC do PostgreSQL
spark = SparkSession.builder \
    .appName("ETL do Banco de Dados") \
    .config("spark.driver.extraClassPath", "C:/Users/elton/Documents/projeto/postgresql-42.7.0.jar") \
    .config("spark.hadoop.home.dir", "C:/Users/elton/Documents/spark-3.5.0-bin-hadoop3") \
    .getOrCreate()
# Imprime informações sobre a sessão do Spark
print("Sessão do Spark criada com sucesso!")
print(f"Versão do Spark: {spark.version}")
print("Classpath do Spark:", spark._jsc.sc().getConf().get("spark.driver.extraClassPath"))
print(spark.sparkContext.getConf().toDebugString())

# URL de conexão JDBC
url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# Propriedades para a conexão JDBC
properties = {
    "user": DB_USERNAME,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

