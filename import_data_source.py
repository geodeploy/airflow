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

    @provide_session
    def recycle_stage(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).filter(XCom.timestamp <= datetime.now(timezone.utc) -
                                                                 timedelta(
                                                                     days=dag.params['days_ago_xcom_retention'])
                                                                 ).delete()

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


    def download_file(setting:dict, **kwargs):
        #
        if not setting['enabled']:
            raise AirflowSkipException('Source not enabled: {}'.format(setting['name']))

        #
        source = setting['source']
        params = source['params']
        conn = Connection.get_connection_from_secrets(conn_id=source['conn_id'])
        params['typeName'] = '{}:{}'.format(source['workspace'], params['typeName'])
        endpoint = '{}/{}'.format(source['workspace'], source['endpoint'])
        protocol = source['protocol']
        if source['method'] == 'GET':
            url_params='&'.join(['{}={}'.format(key, params[key]) for key in params])
            url = '{}://{}/{}?{}'.format(protocol, conn.host, endpoint, url_params)
            data = {}
        elif source['method'] == 'POST':
            url = '{}://{}/{}'.format(protocol, conn.host, endpoint)
            data = params
        else:
            raise AirflowException('invalid http method')
        #
        headers = {}
        if source['auth_type'] == 'basic':
            token = b64encode(f"{conn.login}:{conn.password}".encode('utf-8')).decode("ascii")
            headers = {'Authorization': f'Basic {token}'}

        #
        print('Requesting content from url: {}'.format(url))
        response = requests.request(source['method'], url, headers=headers, data=data, verify=False, timeout=(3600, 3600))

        # write temporary geojson dump file
        temp_folder = os.path.join(tempfile._get_default_tempdir(), next(tempfile._get_candidate_names())).__str__()
        os.mkdir(temp_folder)
        dump_file = os.path.join(temp_folder, "dump_file.geojson")
        print('Saving response content to temporary geojson dump file "{}"'.format(dump_file))
        f = open(file=dump_file, mode='wb')
        f.write(response.content)
        f.close()
        return [{'dump_file': dump_file, 'setting': setting}]


    def load_file_to_database(restore: dict, **kwargs):
        ti = kwargs['ti']

        dump_file = restore['dump_file']

        # open dump file
        file = open(dump_file, newline='')

        # inicia conexão com o datalake no datalake
        pgsql_hook = PostgresHook()
        hook = pgsql_hook.get_hook(conn_id='pgsql_datalake')
        engine = hook.get_sqlalchemy_engine(engine_kwargs={'connect_args': {'connect_timeout': 7200}})

        # load in database
        target = restore['setting']['target']
        print('[Loading geojson to Geodataframe...]')
        gdf = gpd.read_file(filename=restore['dump_file'])

        print('[Loading Geodataframe to PGSQL...]')
        gdf.to_postgis(con=engine, schema=target['schema'], name=target['raw_table'], index=True, if_exists='replace')
        print('[{}] Finished!'.format(datetime.now()))

        # remove temp folder
        rmtree(folder=os.path.dirname(restore['dump_file']))

        del engine

        return [restore['setting']]

    def preprocessing_data(setting: dict, **kwargs):
        gtype_code = {"POLYGON": 3, "linestring": 2, "point": 1}
        target = setting['target']
        gtype = str(target['gtype']).upper()
        schema = target['schema']
        table = target['table']
        raw_table = target['raw_table']
        DDL = """
            DROP TABLE IF EXISTS "{}"."{}";

            CREATE TABLE "{}"."{}" AS
            SELECT
                t.*,
                st_multi(st_collectionextract(t.geom_repaired, {})) AS geom
            FROM (
                SELECT
                    *,
                    st_makevalid(geometry) AS geom_repaired
                FROM
                    "{}"."{}" r
            ) t
            WHERE upper(st_geometrytype(t.geom_repaired)) = ANY (ARRAY['ST_{}'::text, 'ST_MULTI{}'::text, 'ST_GEOMETRYCOLLECTION'::text]);
            
            ALTER TABLE "{}"."{}" DROP COLUMN geometry;
            
            ALTER TABLE "{}"."{}" DROP COLUMN geom_repaired;
            
            CREATE INDEX IF NOT EXISTS si_{}_geom ON "{}"."{}" USING gist (geom) TABLESPACE pg_default;
            
            DROP TABLE IF EXISTS "{}"."{}";
        """.format(schema, table, schema, table, gtype_code[gtype], schema, raw_table, gtype, gtype, schema, table, schema, table, table, schema, table, schema, raw_table)

        # inicia conexão com o datalake no datalake
        pgsql_hook = PostgresHook()
        hook = pgsql_hook.get_hook(conn_id='pgsql_datalake')
        engine = hook.get_sqlalchemy_engine(engine_kwargs={'connect_args': {'connect_timeout': 7200}})

        print('DDL', DDL)

        # executa instrução DDL
        engine.execute(text(DDL))

        # destroi conexão co o banco
        del engine

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

    download_file = PythonOperator.partial(
        task_id='DOWNLOAD_ARQUIVO',
        python_callable=download_file,
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
        trigger_rule='none_failed'
    ).expand(op_args=load_settings.output)

    load_file_to_database = PythonOperator.partial(
        task_id='CARREGAR_DADOS',
        python_callable=load_file_to_database,
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
        trigger_rule='none_failed'
    ).expand(op_args=download_file.output)

    preprocessing_data = PythonOperator.partial(
        task_id='PREPROCESSAR_DADOS',
        python_callable=preprocessing_data,
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
        trigger_rule='none_failed'
    ).expand(op_args=load_file_to_database.output)

    # ***************************************************
    # FLOW
    # ***************************************************

    load_settings >> download_file >> load_file_to_database >> preprocessing_data

