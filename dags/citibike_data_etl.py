from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import zipfile
import os
import psycopg2


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 9, 1),  # Дата начала доступных данных для JC
    'end_date': datetime(2020, 9, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'citibike_data_etl',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
)

def get_data_url(**context):
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    filename = f'JC-{year}{month:02d}-citibike-tripdata.csv.zip'
    url = f'https://s3.amazonaws.com/tripdata/{filename}'

    context['ti'].xcom_push(key='data_url', value=url)
    context['ti'].xcom_push(key='filename', value=filename)


def download_data(**context):
    url = context['ti'].xcom_pull(key='data_url', task_ids='get_data_url')
    filename = context['ti'].xcom_pull(key='filename', task_ids='get_data_url')
    
    # Определяем путь для сохранения скачанного файла
    download_dir = '/opt/airflow/dags/data/downloads'
    os.makedirs(download_dir, exist_ok=True)
    download_path = os.path.join(download_dir, filename)
    
    # Скачиваем файл
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Файл успешно скачан: {download_path}")
    else:
        raise Exception(f"Не удалось скачать файл по адресу {url}. Статус код: {response.status_code}")
    
    # Сохраняем путь к скачанному файлу для использования в следующих задачах
    context['ti'].xcom_push(key='download_path', value=download_path)


def unzip_data(**context):
    download_path = context['ti'].xcom_pull(key='download_path', task_ids='download_data')
    filename = os.path.basename(download_path)
    extract_dir = f'/opt/airflow/dags/data/unzipped/{os.path.splitext(filename)[0]}'
    os.makedirs(extract_dir, exist_ok=True)
    
    # Разархивируем файл
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    print(f"Файл успешно разархивирован в: {extract_dir}")
    
    # Сохраняем путь к разархивированной директории для использования в следующих задачах
    context['ti'].xcom_push(key='extract_dir', value=extract_dir)


def process_data(**context):
    extract_dir = context['ti'].xcom_pull(key='extract_dir', task_ids='unzip_data')
    # Предполагается, что внутри extract_dir находится один CSV-файл
    # Структура: /unzipped/JC-YYYYMM-citibike-tripdata.csv/JC-YYYYMM-citibike-tripdata.csv
    filename = f"{os.path.basename(extract_dir)}"
    file_path = os.path.join(extract_dir, filename)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не найден.")
    
    # Чтение CSV-файла
    df = pd.read_csv(file_path)
    print(f"Исходное количество записей: {len(df)}")
    
    # Нормализация имен столбцов
    df.columns = [col.strip().lower().replace(' ', '').replace('_', '') for col in df.columns]
    
    # Проверка на наличие необходимых столбцов
    required_columns = [
        'tripduration', 'starttime', 'stoptime', 'startstationid',
        'startstationname', 'startstationlatitude', 'startstationlongitude',
        'endstationid', 'endstationname', 'endstationlatitude',
        'endstationlongitude', 'bikeid', 'usertype', 'birthyear', 'gender'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Отсутствуют необходимые столбцы: {missing_columns}")
    
    # Преобразование даты начала поездки в datetime
    df['starttime'] = pd.to_datetime(df['starttime'], errors='coerce')
    
    # Определяем первый понедельник для текущего месяца
    execution_date = context['execution_date']
    first_day = datetime(execution_date.year, execution_date.month, 1)
    days_ahead = 0 if first_day.weekday() == 0 else 7 - first_day.weekday()
    first_monday = first_day + timedelta(days=days_ahead)
    
    # Фильтруем записи, где starttime соответствует первому понедельнику
    filtered_df = df[df['starttime'].dt.date == first_monday.date()]
    print(f"Количество записей после фильтрации первого понедельника: {len(filtered_df)}")
    
    # Очистка данных (например, удаление записей с пропущенными значениями)
    cleaned_df = filtered_df.dropna()
    print(f"Количество записей после очистки: {len(cleaned_df)}")
    
    # Сохранение обработанных данных в отдельную директорию
    processed_dir = '/opt/airflow/dags/data/processed'
    os.makedirs(processed_dir, exist_ok=True)
    processed_file_path = os.path.join(processed_dir, filename)
    cleaned_df.to_csv(processed_file_path, index=False)
    print(f"Обработанные данные сохранены по адресу: {processed_file_path}")
    
    # Сохранение пути к обработанным данным для использования в следующих задачах
    context['ti'].xcom_push(key='processed_file_path', value=processed_file_path)

def load_data(**context):
    processed_file_path = context['ti'].xcom_pull(key='processed_file_path', task_ids='process_data')
    
    if not os.path.exists(processed_file_path):
        raise FileNotFoundError(f"Файл {processed_file_path} не найден.")

    # Чтение CSV-файла
    df = pd.read_csv(processed_file_path)

    # Информация о дате исполнения
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    # Имя партиции
    partition_name = f'p{year}_{month:02d}'
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"

    # Создание новой партиции
    create_partition_sql = f"""
        ALTER TABLE citibike_tripdata
        ADD PARTITION {partition_name} START ('{start_date}') END ('{end_date}');
    """

    # Подключение к Greenplum
    pg_hook = PostgresHook(postgres_conn_id='greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(create_partition_sql)
        conn.commit()
    except psycopg2.errors.DuplicateObject:
        # Партиция уже существует
        conn.rollback()
    
    # Загрузка данных в таблицу
    copy_sql = f"""
        COPY citibike_tripdata FROM STDIN WITH CSV HEADER DELIMITER AS ',';
    """
    with open(processed_file_path, 'r') as f:
        try:
            cursor.copy_expert(copy_sql, f)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise Exception(f"Ошибка при загрузке данных: {e}")

    # Отчетность о загруженных записях
    num_records = len(df)
    Variable.set(f'citibike_records_{year}_{month:02d}', num_records)
    print(f"Количество загруженных записей: {num_records}")

    cursor.close()
    conn.close()

# Определяем задачи DAG
get_data_url_task = PythonOperator(
    task_id='get_data_url',
    python_callable=get_data_url,
    provide_context=True,
    dag=dag,
)

download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    provide_context=True,
    dag=dag,
)

unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Устанавливаем зависимости между задачами
get_data_url_task >> download_data_task >> unzip_data_task >> process_data_task >> load_data_task
