# Using official dbt Docker
FROM python:3.8-slim-buster

# set the working dir
WORKDIR /dbt
COPY dbt/dbt_project.yml /dbt/

COPY . .
COPY dbt/profiles.yml /dbt/


RUN pip install --upgrade pip
RUN pip install dbt-snowflake

RUN dbt

