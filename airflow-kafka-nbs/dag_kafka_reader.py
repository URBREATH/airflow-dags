from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json

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
    """
    Consuma un messaggio dal topic 'analytcs_result' e lo stampa formattato.
    """
    # Configura il consumer per il topic 'analytcs_result'
    consumer = KafkaConsumer(
        'analytcs_result',
        bootstrap_servers=['kafka-broker-1:9092'],  # Usa il nome host interno se in ambiente Docker
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-group'
    )

    # Legge un solo messaggio e lo stampa
    for message in consumer:
        try:
            # Decodifica il messaggio in JSON
            msg = json.loads(message.value.decode('utf-8'))
            print("Message recived from topic analytcs_result:")
            print(f"  Status          : {msg.get('status')}")
            print(f"  Bucket          : {msg.get('bucket')}")
            print(f"  Object Name     : {msg.get('object_name')}")
            print(f"  Original Message: {msg.get('original_message')}")
        except Exception as e:
            print("Error on message:", e)
        break  # Esce dal ciclo dopo il primo messaggio
    consumer.close()


# Definizione del DAG
with DAG(
        'dag_kafka_reader',
        default_args=default_args,
        description='DAG to read message from broker',
        schedule_interval='@once',  # Esecuzione una sola volta; modifica se necessario
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    task_consume = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_kafka_message
    )

    task_consume
