#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE DATABASE airflow_metadata_db;
  CREATE USER airflow_meta_user WITH PASSWORD 'VNXkgKEPBn69yYwA';
  GRANT ALL PRIVILEGES ON DATABASE airflow_metadata_db TO airflow_meta_user;

  CREATE DATABASE celery_results_db;
  CREATE USER celery_user WITH PASSWORD 'L4PYpRNq6mxSQfyj';
  GRANT ALL PRIVILEGES ON DATABASE celery_results_db TO celery_user;

  CREATE DATABASE elt_db;
  CREATE USER yt_api_user WITH PASSWORD 'X57tmQ846GYP3Jgb';
  GRANT ALL PRIVILEGES ON DATABASE elt_db TO yt_api_user;
EOSQL
