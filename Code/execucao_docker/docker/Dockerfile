FROM bitnami/spark:3.5.0

# Instale as dependências do sistema
RUN apt-get update \
    && apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs

# Instale as dependências do PostgreSQL
RUN apt-get install -y libpq-dev

# Instale o psycopg2
RUN pip3 install psycopg2

# Instale o cliente PostgreSQL
RUN apt-get install -y postgresql-client

# Baixe e adicione o driver JDBC do PostgreSQL
RUN wget -P /opt/bitnami/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.0.jar

# Set the working directory
WORKDIR /app

# Copy your application files, including the Spark script
COPY ./main.py main.py
COPY ./seu_script_spark.py /opt/bitnami/spark/scripts/

# Install Python dependencies
RUN python -m pip install --upgrade pip

# If you have a requirements.txt file, uncomment the next line and copy it
# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt

# Configurar a variável de ambiente SPARK_SUBMIT_OPTIONS para incluir o driver JDBC do PostgreSQL
ENV SPARK_SUBMIT_OPTIONS "--driver-class-path /opt/bitnami/spark/jars/postgresql-42.7.0.jar --jars /opt/bitnami/spark/jars/postgresql-42.7.0.jar"

ENTRYPOINT ["python", "main.py"]
