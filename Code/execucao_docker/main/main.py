import logging
import importlib.util
from datetime import datetime
import os
import pandas as pd
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
from sqlalchemy import create_engine

# ... (rest of your existing code)

# Run unit tests
if __name__ == "__main__":
    # Import unittest and test class
    import unittest
    from teste_leitura_associado import TestLeituraAssociado
    from teste_leitura_cartao import TestLeituraCartao
    from teste_leitura_conta import TestLeituraConta
    from teste_leitura_movimento import TestLeituraTabelaMovimento
    from teste_leitura_movimento_flat import TestLeituraTabelaMovimentoFlat
    # Load tests from the test class
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraAssociado)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraCartao)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraConta)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraTabelaMovimento)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestLeituraTabelaMovimentoFlat)
    # Run tests
    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    # Check test results
    if result.wasSuccessful():
        print("All tests passed.")
       
    else:
        print("Some tests failed.")

