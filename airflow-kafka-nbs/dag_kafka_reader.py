from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer

# Impostazioni di default per il DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def consume_kafka_message():
    # Configura il consumer per il topic 'nome_topic'
    consumer = KafkaConsumer(
        'demo_nbs',
        bootstrap_servers=['https://kafka-broker-dev.urbreath.tech/'],  # Modifica con l'indirizzo del tuo broker Kafka
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-group'
    )
    # Legge un solo messaggio e lo stampa
    for message in consumer:
        print(f"Messaggio ricevuto: {message.value.decode('utf-8')}")
        break  # Esce dal ciclo dopo il primo messaggio
    consumer.close()


# Definizione del DAG
with DAG(
        'dag_kafka_reader',
        default_args=default_args,
        description='DAG per leggere messaggi da Kafka',
        schedule_interval='@once',  # Esecuzione una sola volta; modifica se necessario
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    task_consume = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_kafka_message
    )

    task_consume
