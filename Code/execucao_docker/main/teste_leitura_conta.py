import unittest
from pyspark.sql import SparkSession

class TesteLeituraConta(unittest.TestCase):
    def setUp(self):
        # Configuração do Spark para o teste
        self.spark = SparkSession.builder.master("local[2]").appName("TesteLeituraConta").getOrCreate()

    def tearDown(self):
        # Encerra a sessão do Spark após o teste
        self.spark.stop()

    def test_leitura_parquet(self):
        # Caminho para o arquivo Parquet gerado
        caminho_parquet = "stg_conta.parquet"

        # Leitura do arquivo Parquet como um DataFrame do Spark
        df_conta = self.spark.read.parquet(caminho_parquet)

        # Asserts para validar o DataFrame
        self.assertIsNotNone(df_conta)
        self.assertTrue("id_associado" in df_conta.columns)
        self.assertEqual(df_conta.count(),  10)

if __name__ == '__main__':
    unittest.main()
