import logging
import os.path
import json
import sys
import base64
import csv
import requests
import hashlib
import base64
import tempfile
import shapefile
import pandas as pd
import urllib.request
from glob import glob
from zipfile import ZipFile
import geopandas as gpd
from base64 import b64encode
from datetime import timedelta, datetime, date, timezone
from airflow import DAG
from airflow.models import Connection
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from libs.util import csv_to_pgsql, jsonfile_to_dict, rmtree, save_to_file
from sqlalchemy import text


def failure_callback(context):
    pass


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['iuri@geodeploy.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    "on_failure_callback": failure_callback,
    'start_date': datetime(2023, 1, 1),
    'retries': None,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        dag_id='IMPORTAR_FONTE_DADOS',
        default_args=default_args,
        user_defined_filters={'fromjson': lambda s: json.loads(s)},
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=timedelta(hours=4),
        tags=["Datalake", "ASV", "CEFIR"],
        params=jsonfile_to_dict(input_path='%s/dags/vars/import_data_source.json' % os.getcwd())
) as dag:

    # ***************************************************
    # FUNCTIONS
    # ***************************************************


    def load_settings(**kwargs):
        try:
            settings = json.loads(json.dumps(dag.params['settings']))
            if kwargs['packing']:
                settings = [[setting] for setting in settings]
            else:
                settings = [setting for setting in settings]
            return settings
        except Exception as e:
            raise e
        finally:
            pass



    # ***************************************************
    # OPERATORS
    # ***************************************************

    load_settings = PythonOperator(
        task_id='CARREGAR_CONFIG',
        python_callable=load_settings,
        op_kwargs={'packing': True},
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag
    )


    # ***************************************************
    # FLOW
    # ***************************************************

    load_settings

