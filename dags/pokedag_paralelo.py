from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError
logging.getLogger("airflow.providers.http").setLevel(logging.WARNING)

POKEMON_LIMIT = 1000
OUTPUT_PATH = "/tmp/pokemon_data/pokemon_base.csv"
POKEMON_DATA_PATH = "/tmp/pokemon_data/pokemon_data.json"
SPECIES_DATA_PATH = "/tmp/pokemon_data/species_data.json"
MERGED_DATA_PATH = "/tmp/pokemon_data/pokemon_merged.csv"

default_args = {
    'owner': 'pablo',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Tarea A: /pokemon/{id}
def download_pokemon_data(**kwargs):
    urls = [item['url'] for item in json.loads(kwargs['ti'].xcom_pull(task_ids='fetch_pokemon_list'))['results']]
    hook = HttpHook(http_conn_id='pokeapi', method='GET')
    pokemon_data = []
    try:
        for i, url in enumerate(urls):
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon/{pokemon_id}/"
            res = hook.run(endpoint)
            pokemon_data.append(res.json())
            time.sleep(0.5)
            if (i + 1) % 100 == 0:
                logging.info(f"[INFO] {i + 1} Pokémon descargados")
    except Exception as e:
        logging.error(f"[ERROR] Interrupción al descargar Pokémon: {e}")
        os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)
        with open(POKEMON_DATA_PATH, 'w') as f:
            json.dump(pokemon_data, f)
        raise e

    os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)
    with open(POKEMON_DATA_PATH, 'w') as f:
        json.dump(pokemon_data, f)

# Tarea B: /pokemon-species/{id}
def download_species_data(**kwargs):
    urls = [item['url'] for item in json.loads(kwargs['ti'].xcom_pull(task_ids='fetch_pokemon_list'))['results']]
    hook = HttpHook(http_conn_id='pokeapi', method='GET')
    species_data = []
    try:
        for i, url in enumerate(urls):
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon-species/{pokemon_id}/"
            res = hook.run(endpoint)
            species = res.json()
            species_data.append({
                'name': species['name'],
                'generation': species['generation']['name']
            })
            time.sleep(0.5)
            if (i + 1) % 100 == 0:
                logging.info(f"[INFO] {i + 1} species descargadas")
    except Exception as e:
        logging.error(f"[ERROR] Interrupción al descargar species: {e}")
        os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)
        with open(SPECIES_DATA_PATH, 'w') as f:
            json.dump(species_data, f)
        raise e

    os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)
    with open(SPECIES_DATA_PATH, 'w') as f:
        json.dump(species_data, f)

# Tarea C: combinar y transformar
def merge_and_transform_data(**kwargs):
    with open(POKEMON_DATA_PATH, 'r') as f:
        pokemon_data = json.load(f)
    with open(SPECIES_DATA_PATH, 'r') as f:
        species_data = json.load(f)
    species_lookup = {s['name']: s['generation'] for s in species_data}
    tidy_records = []
    for p in pokemon_data:
        p['generation'] = species_lookup.get(p['name'])  # puede quedar como None
        stats = {s['stat']['name']: s['base_stat'] for s in p.get('stats', [])}
        types = sorted(p.get('types', []), key=lambda t: t['slot'])
        tidy_records.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "base_experience": p.get("base_experience"),
            "generation": p.get("generation"),
            "type_1": types[0]['type']['name'] if len(types) > 0 else None,
            "type_2": types[1]['type']['name'] if len(types) > 1 else None,
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special-attack": stats.get("special-attack"),
            "special-defense": stats.get("special-defense"),
            "speed": stats.get("speed"),
        })
    df = pd.DataFrame(tidy_records)
    os.makedirs(os.path.dirname(MERGED_DATA_PATH), exist_ok=True)
    df.to_csv(MERGED_DATA_PATH, index=False)
    print(f"[INFO] CSV guardado en: {MERGED_DATA_PATH}")

# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    fetch_pokemon_list = HttpOperator(
        task_id='fetch_pokemon_list',
        http_conn_id='pokeapi',
        endpoint=f'/pokemon?limit={POKEMON_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    download_a = PythonOperator(
        task_id='download_pokemon_data',
        python_callable=download_pokemon_data,
    )

    download_b = PythonOperator(
        task_id='download_species_data',
        python_callable=download_species_data,
    )

    merge_transform = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )

    fetch_pokemon_list >> [download_a, download_b] >> merge_transform
