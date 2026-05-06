from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from helpers.pipeline_exemplo_kaggle.pipeline_bronze import pipeline_kaggle_bronze
from helpers.pipeline_exemplo_kaggle.pipeline_silver import pipeline_kaggle_silver
from helpers.pipeline_exemplo_kaggle.pipeline_gold import pipeline_kaggle_gold

# Argumentos padrão
args = {
    "start_date": datetime(2024, 12, 24),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

# Definição da DAG
dag = DAG(
    dag_id="pipeline_exemplo_kaggle",
    default_args=args,
    description="pipeline_exemplo_kaggle",
    schedule_interval=None,
    start_date=datetime(2024, 12, 30),
    tags=["Exemplo", "Kaggle"],
    catchup=False,
)


# Data atual (melhor evitar datetime.now() fora de execução real,
# mas mantendo simples conforme seu exemplo)
now = datetime.now()
year = now.year
month = str(now.month).zfill(2)
day = str(now.day).zfill(2)

# Caminho base
path_kaggle_exemplo = "/opt/airflow/datasets/Kaggle"

# Lista de arquivos
file_names = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
]

previous_task = None

# Loop para criação dinâmica das tasks
for file_name in file_names:

    source_path = f"{path_kaggle_exemplo}/{file_name}"
    pipeline_kaggle_bronze_task = PythonOperator(
        task_id=f"bronze_{file_name}",
        python_callable=pipeline_kaggle_bronze,
        op_kwargs={
            "source_path": source_path,
            "file_name": file_name,
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        dag=dag,
    )
    
    
    pipeline_kaggle_silver_task = PythonOperator(
        task_id=f"silver_{file_name}",
        python_callable=pipeline_kaggle_silver,
        op_kwargs={
            "file_name": file_name
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        dag=dag,
    )
    
    dummy_name = file_name.replace('.csv','')
    dummy_name = f'dummy_{dummy_name}'
    dummy = EmptyOperator(task_id=dummy_name, dag=dag)

    pipeline_kaggle_bronze_task >> pipeline_kaggle_silver_task >> dummy
    
    # Garante que as tasks sejam executadas em sequencia
    if previous_task:
        previous_task >> pipeline_kaggle_bronze_task
    
    # atualiza a task anterior para  apróxima autalização
    # criando uma sequencia de tasks
    previous_task = dummy

pipeline_kaggle_gold_task = PythonOperator(
        task_id=f"gold_kaggle",
        python_callable=pipeline_kaggle_gold,
        retries=2,
        retry_delay=timedelta(minutes=5),
        dag=dag,
    )

previous_task >> pipeline_kaggle_gold_task
