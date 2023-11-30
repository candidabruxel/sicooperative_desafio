import logging
import importlib.util
from datetime import datetime
import os
import warnings
import sys
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import main.functions as func
import stg_associado, stg_cartao, stg_conta, stg_movimento, flat_movimento
from teste_leitura_associado import TestLeituraAssociado
from teste_leitura_cartao import TestLeituraCartao
from teste_leitura_conta import TestLeituraConta
from teste_leitura_movimento import TestLeituraTabelaMovimento
from teste_leitura_movimento_flat import TestLeituraTabelaMovimentoFlat

warnings.filterwarnings('ignore')

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Definir caminhos e sistema
if sys.platform == 'win32':
    logging.info('Executando no Windows...')
    path_main = os.getcwd() + '\\main\\'
    path_local = os.getcwd() + '\\'
elif sys.platform == 'linux':
    logging.info('Executando no Linux...')
    path_main = '/app/SICOOPERATIVE/Code/execucao_docker/main/'
    path_local = '/app/SICOOPERATIVE/Code/execucao_docker/'
else:
    logging.error('Não foi possível identificar o sistema.')
    raise SystemExit

# Executar as funções principais
stg_associado.main()
stg_cartao.main()
stg_conta.main()
stg_movimento.main()
flat_movimento.main()

# Executar os testes
if __name__ == "__main__":
    # Carregar testes
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraAssociado)
    suite.addTests(loader.loadTestsFromTestCase(TestLeituraCartao))
    suite.addTests(loader.loadTestsFromTestCase(TestLeituraConta))
    suite.addTests(loader.loadTestsFromTestCase(TestLeituraTabelaMovimento))
    suite.addTests(loader.loadTestsFromTestCase(TestLeituraTabelaMovimentoFlat))

    # Rodar testes
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Checar resultados dos testes
    if result.wasSuccessful():
        logging.info("Todos os testes passaram.")
    else:
        logging.error("Alguns testes falharam.")
