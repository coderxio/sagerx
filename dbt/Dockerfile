FROM python:3.11-slim

ENV DBT_PROFILES_DIR=/dbt

RUN apt-get -y update
RUN apt-get -y install git

RUN pip install dbt-core dbt-postgres

WORKDIR /dbt

COPY . .

WORKDIR /dbt/sagerx