import unittest
from pyspark.sql import SparkSession

class TesteLeituraAssociado(unittest.TestCase):
    def setUp(self):
        # Configuração do Spark para o teste
        self.spark = SparkSession.builder.master("local[2]").appName("TesteLeituraAssociado").getOrCreate()

    def tearDown(self):
        # Encerra a sessão do Spark após o teste
        self.spark.stop()

    def test_leitura_parquet(self):
        # Caminho para o arquivo Parquet gerado
        caminho_parquet = "stg_associado.parquet"

        # Leitura do arquivo Parquet como um DataFrame do Spark
        df_associado = self.spark.read.parquet(caminho_parquet)

        # Asserts para validar o DataFrame
        self.assertIsNotNone(df_associado)
        self.assertTrue("nome" in df_associado.columns)
        self.assertEqual(df_associado.count(),  10)

if __name__ == '__main__':
    unittest.main()
