import logging
import importlib.util
from datetime import datetime
import os
import warnings
import sys
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import stg_associado
import stg_cartao
import stg_conta
import stg_movimento

warnings.filterwarnings('ignore')

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Definir caminhos e sistema
if sys.platform == 'win32':
    logging.info('Executando no Windows...')
    path_main = os.getcwd() + '\\'
elif sys.platform == 'linux':
    logging.info('Executando no Linux...')
    path_main = '/app/SICOOPERATIVE/Code/execucao_docker/'
else:
    logging.error('Não foi possível identificar o sistema.')
    raise SystemExit

# Executar as funções principais
stg_associado.main()
stg_cartao.main()
stg_conta.main()
stg_movimento.main()

import flat_movimento
flat_movimento.main()

from teste_leitura_associado import TesteLeituraAssociado
from teste_leitura_cartao import TesteLeituraCartao
from teste_leitura_conta import TesteLeituraConta
from teste_leitura_movimento import TesteLeituraMovimento
from teste_leitura_movimento_flat import TesteLeituraFlatMovimento

# Executar os testes
if __name__ == "__main__":
     #Carregar testes
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TesteLeituraAssociado)
    suite.addTests(loader.loadTestsFromTestCase(TesteLeituraCartao))
    suite.addTests(loader.loadTestsFromTestCase(TesteLeituraConta))
    suite.addTests(loader.loadTestsFromTestCase(TesteLeituraMovimento))
    suite.addTests(loader.loadTestsFromTestCase(TesteLeituraFlatMovimento))

    # Rodar testes
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Checar resultados dos testes
if result.wasSuccessful():
   logging.info("Todos os testes passaram.")
else:
    logging.error("Alguns testes falharam.")
