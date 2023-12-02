FROM bitnami/spark:3.5.0

#ambiente virtual em um diretório com permissões adequadas
RUN mkdir /tmp/app_venv && python -m venv /tmp/app_venv
ENV PATH="/tmp/app_venv/bin:$PATH"

#dependências do sistema
USER root
RUN mkdir -p /var/lib/apt/lists/partial \
    && apt-get update \
    && apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs

# driver JDBC do PostgreSQL
RUN apt-get install -y curl && \
    curl -o /opt/bitnami/spark/jars/postgresql-42.7.0.jar https://jdbc.postgresql.org/download/postgresql-42.7.0.jar
RUN mkdir -p /opt/bitnami/spark/data

# dependências do PostgreSQL
RUN apt-get install -y libpq-dev

# Instala o psycopg2
RUN pip3 install psycopg2

# cliente PostgreSQL
RUN apt-get install -y postgresql-client
# Install Python dependencies
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install pyarrow  
# Set the working directory
WORKDIR /app

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

RUN python -m pip install --upgrade pip
RUN pip install py4j

# Configurar a variável de ambiente SPARK_SUBMIT_OPTIONS para incluir o driver JDBC do PostgreSQL
ENV SPARK_SUBMIT_OPTIONS "--driver-class-path /opt/bitnami/spark/jars/postgresql-42.7.0.jar --jars /opt/bitnami/spark/jars/postgresql-42.7.0.jar"

ENTRYPOINT ["python", "main.py"]