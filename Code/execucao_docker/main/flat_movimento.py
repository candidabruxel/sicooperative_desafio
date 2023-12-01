from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pandas as pd
from sqlalchemy import create_engine



DB_USERNAME = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_DATABASE = 'desafio'



# Configuração do Spark com o driver JDBC do PostgreSQL
spark = SparkSession.builder \
    .appName("ETL do Banco de Dados") \
    .getOrCreate()

# ETL para criação da tabela movimento_flat)
df_dim_associado = spark.read.parquet("/opt/bitnami/spark/data/stg_associado.parquet")
df_dim_conta = spark.read.parquet("/opt/bitnami/spark/data/stg_conta.parquet")
df_dim_cartao = spark.read.parquet("/opt/bitnami/spark/data/stg_cartao.parquet")
df_dim_movimento = spark.read.parquet("/opt/bitnami/spark/data/stg_movimento.parquet")

df_movimento_flat = df_dim_associado.alias("associado") \
    .join(df_dim_conta.alias("conta"), col("associado.id") == col("conta.id_associado")) \
    .join(df_dim_cartao.alias("cartao"), col("conta.id") == col("cartao.id_conta")) \
    .join(df_dim_movimento.alias("movimento"), col("cartao.id") == col("movimento.id_cartao")) \
    .select(
        col("associado.nome").alias("nome_associado"),
        col("associado.sobrenome").alias("sobrenome_associado"),
        col("associado.idade").alias("idade_associado"),
        col("movimento.vls_transacao").alias("vlr_transacao_movimento"),
        col("movimento.des_transacao").alias("des_transacao_movimento"),
        col("movimento.data_movimento"),
        col("cartao.num_cartao").alias("numero_cartao"),
        col("cartao.nom_impresso").alias("nome_impresso_cartao"),
        col("conta.tipo"),
        col("conta.data_criacao").alias("data_criacao_conta")
    )

df_movimento_flat.write.format("parquet").mode("overwrite").save("movimento_flat")