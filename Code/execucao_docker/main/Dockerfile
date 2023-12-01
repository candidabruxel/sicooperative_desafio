FROM bitnami/spark:3.5.0

# Crie e ative um ambiente virtual em um diretório com permissões adequadas
RUN mkdir /tmp/app_venv && python -m venv /tmp/app_venv
ENV PATH="/tmp/app_venv/bin:$PATH"
# Instale as dependências do sistema
#RUN apt-get update \
 #   && apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs
USER root
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs

# Instale as dependências do sistema
RUN mkdir -p /var/lib/apt/lists/partial \
    && apt-get update \
    && apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs

# Baixe e adicione o driver JDBC do PostgreSQL
RUN apt-get install -y wget && \
    wget -P /opt/bitnami/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.0.jar

RUN pip install pandas
# Instalar o sqlalchemy
RUN pip install sqlalchemy

# Instale as dependências do PostgreSQL
RUN apt-get install -y libpq-dev

# Instale o psycopg2
RUN pip3 install psycopg2

# Instale o cliente PostgreSQL
RUN apt-get install -y postgresql-client

# Baixe e adicione o driver JDBC do PostgreSQL
RUN wget -P /opt/bitnami/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.0.jar

RUN mkdir -p /opt/bitnami/spark/data
# Set the working directory
WORKDIR /app

# Copy your application files, including the Spark script
COPY main.py .
COPY stg_associado.py .
COPY stg_cartao.py .
COPY stg_conta.py .
COPY stg_movimento.py .
COPY flat_movimento.py .
COPY teste_leitura_associado.py .
COPY teste_leitura_cartao.py .
COPY teste_leitura_conta.py .
COPY teste_leitura_movimento.py .
COPY teste_leitura_movimento_flat.py .
COPY ./main.py main.py /opt/bitnami/spark/scripts/
COPY ./stg_associado.py stg_associado.py /opt/bitnami/spark/scripts/
COPY ./stg_cartao.py stg_cartao.py /opt/bitnami/spark/scripts/
COPY ./stg_conta.py stg_conta.py /opt/bitnami/spark/scripts/
COPY ./stg_movimento.py stg_movimento.py /opt/bitnami/spark/scripts/
COPY ./flat_movimento.py flat_movimento.py /opt/bitnami/spark/scripts/
COPY ./teste_leitura_associado.py teste_leitura_associado.py /opt/bitnami/spark/scripts/
COPY ./teste_leitura_cartao.py teste_leitura_cartao.py /opt/bitnami/spark/scripts/
COPY ./teste_leitura_conta.py teste_leitura_conta.py /opt/bitnami/spark/scripts/
COPY ./teste_leitura_movimento.py teste_leitura_movimento.py /opt/bitnami/spark/scripts/
COPY ./teste_leitura_movimento_flat.py teste_leitura_movimento_flat.py /opt/bitnami/spark/scripts/

# Install Python dependencies
RUN python -m pip install --upgrade pip
RUN pip install py4j
# If you have a requirements.txt file, uncomment the next line and copy it
# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt

# Configurar a variável de ambiente SPARK_SUBMIT_OPTIONS para incluir o driver JDBC do PostgreSQL
ENV SPARK_SUBMIT_OPTIONS "--driver-class-path /opt/bitnami/spark/jars/postgresql-42.7.0.jar --jars /opt/bitnami/spark/jars/postgresql-42.7.0.jar"

ENTRYPOINT ["python", "main.py"]
