# test_leitura_tabela_associado.py
import unittest
from unittest.mock import MagicMock
from tabela.leitura_tabela import ler_tabela_conta
from sqlalchemy import create_engine

DB_USERNAME = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_DATABASE = 'desafio'

# Conexão usando SQLAlchemy
string_conexao = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# Crie uma engine usando a string de conexão
engine = create_engine(string_conexao)

# URL de conexão JDBC
url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

# Propriedades para a conexão JDBC
properties = {
    "user": DB_USERNAME,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

class TestLeituraTabelaConta(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()

    def test_ler_tabela_conta(self):
        result = ler_tabela_conta(self.spark, url, properties)
        self.assertTrue(result is not None)

