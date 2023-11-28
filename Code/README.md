# Desafio Sicredi


## Dificuldades
O primeiro desafio foi conseguir utilizar o write.parquet para salvar o arquivo local na máquina, tive problemas com permissões e acabei obtando salvar os arquivos com toPandas devido ao prazo, sei que o toPanda pode ser tornar mais lento se utilizado com grande volume de dados, mas para esse cenario funcionou bem.

O segundo desafio esta sendo montar o ambiente docker para testar. Já usei docker via airflow e sem testes unitarios.


## Escolha da estrutura

1 Bando de dados: Estou habituada a utilizar oracle, mas escolhi utilizar PostgreSQL pela facilidade de utilização e instalação local.
2 Linguagem: Escolhi python por ser a linguagem que mais utilizo hoje
3 ETL dos dados: Visto a linguem, optei por spark e assim utilizar pyspark

## Pontos importantes
Na pasta execucao_local há um notebook com o código para leitura do banco, processamento ETL e escrita do arquivo movimento_flat. Algo simples e que atende a demanda.

Na pasta execucao_docker está os codigos para execução em docker da leitura do banco, processamento ETL e escrita da tabela final.





