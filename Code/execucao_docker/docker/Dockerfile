FROM python:3.8.8

# Install system dependencies
RUN apt-get update \
    && apt-get install -y nano gcc libc-dev g++ libffi-dev libxml2 libffi-dev unixodbc-dev libgssapi-krb5-2 libkrb5-3 libatomic1 git-lfs

# Install PostgreSQL dependencies
RUN apt-get install -y libpq-dev

# Install psycopg2 
 RUN pip3 install psycopg2

# Installing PostgreSQL client
RUN apt-get install -y postgresql-client

# Set the working directory
WORKDIR /app

# Copy your application files
COPY ./main.py main.py

# Install Python dependencies
RUN python -m pip install --upgrade pip

# If you have a requirements.txt file, uncomment the next line and copy it
# COPY requirements.txt requirements.txt
# RUN pip install -r requirements.txt
