import unittest
from pyspark.sql import SparkSession

class TesteLeituraMovimento(unittest.TestCase):
    def setUp(self):
        # Configuração do Spark para o teste
        self.spark = SparkSession.builder.master("local[2]").appName("TesteLeituraMovimento").getOrCreate()

    def tearDown(self):
        # Encerra a sessão do Spark após o teste
        self.spark.stop()

    def test_leitura_parquet(self):
        # Caminho para o arquivo Parquet gerado
        caminho_parquet = "stg_movimento"

        # Leitura do arquivo Parquet como um DataFrame do Spark
        df_movimento = self.spark.read.parquet(caminho_parquet)

        # Asserts para validar o DataFrame
        self.assertIsNotNone(df_movimento)
        self.assertTrue("id_cartao" in df_movimento.columns)
        self.assertEqual(df_movimento.count(),  10)

if __name__ == '__main__':
    unittest.main()
