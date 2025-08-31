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

# Extras para ZIP y correo
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

# ========= Config =========
POKEMON_LIMIT = 1000
DATA_DIR = "/tmp/pokemon_data"
POKEMON_DATA_PATH = f"{DATA_DIR}/pokemon_data.json"
SPECIES_DATA_PATH = f"{DATA_DIR}/species_data.json"
MERGED_DATA_PATH = f"{DATA_DIR}/pokemon_merged.csv"  # intermedio
OUTPUT_DIR = "/usr/local/airflow/output"             # fijo y consistente
GROUP_NAME = "Grupo 7"                                # <-- tu grupo
DAG_ID = "pokemon_base_etl_parallel"

default_args = {
    'owner': 'Maria',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# ========= Tarea A: /pokemon/{id} =========
def download_pokemon_data(**kwargs):
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)

    if os.path.exists(POKEMON_DATA_PATH):
        with open(POKEMON_DATA_PATH, 'r') as f:
            pokemon_data = json.load(f)
        done_names = {p['name'] for p in pokemon_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} pokémon descargados.")
    else:
        pokemon_data, done_names = [], set()

    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        for i, entry in enumerate(results):
            name = entry['name']
            if name in done_names:
                continue
            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon/{pokemon_id}/"
            res = hook.run(endpoint)
            pokemon = res.json()
            pokemon_data.append(pokemon)
            done_names.add(name)

            if (i + 1) % 100 == 0:
                with open(POKEMON_DATA_PATH, 'w') as f:
                    json.dump(pokemon_data, f)
                logging.info(f"[INFO] {i + 1} pokémon procesados (hasta ahora {len(pokemon_data)} guardados)")

            time.sleep(0.5)
    except Exception as e:
        logging.error(f"[ERROR] Interrupción en pokémon: {e}")
        with open(POKEMON_DATA_PATH, 'w') as f:
            json.dump(pokemon_data, f)
        raise e

    with open(POKEMON_DATA_PATH, 'w') as f:
        json.dump(pokemon_data, f)
    logging.info(f"[INFO] Descarga finalizada con {len(pokemon_data)} pokémon.")

# ========= Tarea B: /pokemon-species/{id} =========
def download_species_data(**kwargs):
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)

    if os.path.exists(SPECIES_DATA_PATH):
        with open(SPECIES_DATA_PATH, 'r') as f:
            species_data = json.load(f)
        done_names = {s['name'] for s in species_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} species descargadas.")
    else:
        species_data, done_names = [], set()

    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        for i, entry in enumerate(results):
            name = entry['name']
            if name in done_names:
                continue
            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon-species/{pokemon_id}/"
            res = hook.run(endpoint)
            species = res.json()
            species_data.append({
                'name': species['name'],
                'generation': species['generation']['name'],
                'is_legendary': species['is_legendary']
            })
            done_names.add(species['name'])

            if (i + 1) % 100 == 0:
                with open(SPECIES_DATA_PATH, 'w') as f:
                    json.dump(species_data, f)
                logging.info(f"[INFO] {i + 1} species procesadas (hasta ahora {len(species_data)} guardadas)")

            time.sleep(0.5)
    except Exception as e:
        logging.error(f"[ERROR] Interrupción en species: {e}")
        with open(SPECIES_DATA_PATH, 'w') as f:
            json.dump(species_data, f)
        raise e

    with open(SPECIES_DATA_PATH, 'w') as f:
        json.dump(species_data, f)
    logging.info(f"[INFO] Descarga finalizada con {len(species_data)} species.")

# ========= Tarea C: merge + columna 'grupo' + CSV en output/final_{{ ds }}.csv =========
def merge_and_transform_data(**kwargs):
    ds = kwargs["ds"]  # fecha de ejecución YYYY-MM-DD

    with open(POKEMON_DATA_PATH, 'r') as f:
        pokemon_data = json.load(f)
    with open(SPECIES_DATA_PATH, 'r') as f:
        species_data = json.load(f)

    species_lookup = {s['name']: {'generation': s['generation'], 'is_legendary': s['is_legendary']} for s in species_data}
    tidy_records = []
    for p in pokemon_data:
        p_info = species_lookup.get(p['name'], {})
        stats = {s['stat']['name']: s['base_stat'] for s in p.get('stats', [])}
        types = sorted(p.get('types', []), key=lambda t: t['slot'])
        tidy_records.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "base_experience": p.get("base_experience"),
            "generation": p_info.get("generation"),
            "is_legendary": p_info.get("is_legendary", False),
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
    df["grupo"] = GROUP_NAME  # << NUEVO

    # Guardado intermedio (opcional) y final requerido
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(MERGED_DATA_PATH, index=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final_path = os.path.join(OUTPUT_DIR, f"final_{ds}.csv")
    df.to_csv(final_path, index=False)
    print(f"[INFO] CSV final guardado en: {final_path}")

# ========= Tarea D: ZIP de logs reales =========
def exportar_logs_reales_zip(**kwargs):
    ds = kwargs["ds"]
    # Ruta estándar de logs en muchas instalaciones oficiales
    logs_dir = f"/usr/local/airflow/logs/dag_id={DAG_ID}"
    if not os.path.exists(logs_dir):
        alt = f"/opt/airflow/logs/{DAG_ID}"
        logs_dir = alt if os.path.exists(alt) else logs_dir

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    zip_base = os.path.join(OUTPUT_DIR, f"logs_{ds}")  # make_archive agrega .zip
    if os.path.exists(zip_base + ".zip"):
        os.remove(zip_base + ".zip")
    shutil.make_archive(zip_base, 'zip', logs_dir)
    print(f"[INFO] ZIP de logs creado: {zip_base}.zip")

# ========= Tarea E: Enviar correo con adjuntos (SMTP_USER/PASSWORD) =========
def enviar_correo_manual(**kwargs):
    ds = kwargs["ds"]
    csv_path = os.path.join(OUTPUT_DIR, f"final_{ds}.csv")
    zip_path = os.path.join(OUTPUT_DIR, f"logs_{ds}.zip")

    # Chequeo de adjuntos
    for p in [csv_path, zip_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Falta adjunto: {p}")

    to_addr = "cienciadedatos.frm.utn@gmail.com"
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    if not smtp_user or not smtp_pass:
        raise RuntimeError("Faltan variables de entorno SMTP_USER / SMTP_PASSWORD.")

    subject = f"Entrega {GROUP_NAME} - {ds}"
    body = (
        f"Hola,\n\nAdjuntamos la entrega de {GROUP_NAME} correspondiente a {ds}.\n\n"
        f"Archivos:\n- final_{ds}.csv\n- logs_{ds}.zip\n\n"
        f"Enviado automáticamente desde Airflow."
    )

    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Adjuntar CSV
    with open(csv_path, "rb") as f:
        part_csv = MIMEBase("application", "octet-stream")
        part_csv.set_payload(f.read())
    encoders.encode_base64(part_csv)
    part_csv.add_header("Content-Disposition", f'attachment; filename="final_{ds}.csv"')
    msg.attach(part_csv)

    # Adjuntar ZIP
    with open(zip_path, "rb") as f:
        part_zip = MIMEBase("application", "zip")
        part_zip.set_payload(f.read())
    encoders.encode_base64(part_zip)
    part_zip.add_header("Content-Disposition", f'attachment; filename="logs_{ds}.zip"')
    msg.attach(part_zip)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_addr], msg.as_string())

    logging.info(f"[INFO] Email enviado a {to_addr} con adjuntos.")

# ========= DAG =========
with DAG(
    dag_id=DAG_ID,
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species + entrega',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl', 'entrega']
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

    zip_logs = PythonOperator(
        task_id='exportar_logs_reales_zip',
        python_callable=exportar_logs_reales_zip,
    )

    send_email = PythonOperator(
        task_id='enviar_correo_manual',
        python_callable=enviar_correo_manual,
    )

    fetch_pokemon_list >> [download_a, download_b] >> merge_transform >> zip_logs >> send_email

