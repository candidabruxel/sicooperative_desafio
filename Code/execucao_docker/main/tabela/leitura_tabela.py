from pyspark.sql import SparkSession

# Inicia Spark 
spark = SparkSession.builder \
    .appName("ETL do Banco de Dados") \
    .getOrCreate()

def ler_tabela_associado(url: str, properties: dict):
    df_associado_raw = spark.read.jdbc(url=url, table="associado", properties=properties)
    return df_associado_raw

def ler_tabela_conta(url: str, properties: dict):
    df_conta_raw = spark.read.jdbc(url=url, table="conta", properties=properties)
    return df_conta_raw

def ler_tabela_cartao(url: str, properties: dict):
    df_cartao_raw = spark.read.jdbc(url=url, table="cartao", properties=properties)
    return df_cartao_raw

def ler_tabela_movimento(url: str, properties: dict):
    df_movimento_raw = spark.read.jdbc(url=url, table="movimento", properties=properties)
    return df_movimento_raw
