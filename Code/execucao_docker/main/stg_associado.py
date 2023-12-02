from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine

def main():

    DB_USERNAME = 'postgres'
    DB_PASSWORD = 'admin'
    DB_HOST = '192.168.2.107'
    DB_PORT = '5432'
    DB_DATABASE = 'desafio'

    # Conexão usando SQLAlchemy
    string_conexao = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
    

    # Tente imprimir a string de conexão para verificar se ela está correta
    print(string_conexao)

    # Crie uma engine usando a string de conexão
    engine = create_engine(string_conexao)

    # Configuração do Spark com o driver JDBC do PostgreSQL
    spark = SparkSession.builder \
        .appName("ETL do Banco de Dados") \
        .getOrCreate()

    # URL de conexão JDBC
    url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

    # Propriedades para a conexão JDBC
    properties = {
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # Leitura da tabela cartao como um DataFrame (Zona Bruta)
    df_associado_raw = spark.read.jdbc(url=url, table="associado", properties=properties)

    df_associado_raw.write.format("parquet").mode("overwrite").save("stg_associado")

    print("criou stg_associado.parquet")
    #stg_associado.show
if __name__ == '__main__':
    main()