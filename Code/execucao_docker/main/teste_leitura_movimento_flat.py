import unittest
from pyspark.sql import SparkSession

class TesteLeituraFlatMovimento(unittest.TestCase):
    def setUp(self):
        # Configuração do Spark para o teste
        self.spark = SparkSession.builder.master("local[2]").appName("TesteLeituraFlatMovimento").getOrCreate()

    def tearDown(self):
        # Encerra a sessão do Spark após o teste
        self.spark.stop()

    def test_leitura_parquet(self):
        # Caminho para o arquivo Parquet gerado
        caminho_parquet = "movimento_flat"

        # Leitura do arquivo Parquet como um DataFrame do Spark
        
        df_movimento_flat = self.spark.read.parquet(caminho_parquet)

        # Asserts para validar o DataFrame
        self.assertIsNotNone(df_movimento_flat)
        self.assertTrue("nome_associado" in df_movimento_flat.columns)
        self.assertEqual(df_movimento_flat.count(),  10)

if __name__ == '__main__':
    unittest.main()
