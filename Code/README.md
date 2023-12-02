# Desafio Sicredi

## Escolha da estrutura

1. Bando de dados: Estou habituada a utilizar oracle, mas escolhi utilizar PostgreSQL pela facilidade de utilização e instalação local.
2. Linguagem: Escolhi python por ser a linguagem que mais utilizo atualmente.
3. ETL dos dados: Visto a linguagem, optei por spark e assim utilizar pyspark.

## Pontos importantes
Na pasta execucao_local há um notebook com o código para leitura do banco, processamento ETL e escrita do arquivo movimento_flat. Algo simples e que atende a demanda.

Na pasta execucao_docker está os codigos para execução em docker, da leitura do banco, processamento ETL e escrita da tabela final. Assim como os testes unitários.

## Dificuldades
Um dos desafios que tive, foi utilizar o metodo write.parquet ou write.format("parquet") para salvar o arquivo local na máquina, tive problemas com permissões e acabei optando por salvar os arquivos com toPandas, sei que o toPandas pode ser tornar mais lento se utilizado com grande volume de dados, mas para esse cenário funcionou bem. Contudo, ao utilizar docker consegui utilizar o write.format("parquet") normalmente.

Outro desafio foi montar o ambiente docker local para testar o código e testes unitários. Até o momento apenas havia usado docker via airflow e sem testes unitários, apenas com o intuito de deixar automatizado a instalação de versão de bibliotecas e afins. 

Por fim, como spark é algo que não tenho experiencia, atualmente utilizo a ferramenta talend e python 'puro' para ETL, foi um desafio e um aprendizado muito bom utilizar pyspark. Assim como as demais dificuldades, me trouxeram conhecimento.

## Considerações
Para este projeto foi utilizado um notebook emprestado, devido um incidente com o meu, com isso é possivel verificar o nome de outro usuario nos diretorios locais do arquivo projeto.ipynb



