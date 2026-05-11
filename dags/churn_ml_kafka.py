from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import pickle
import json
import base64
import time
import os
import threading
import sched
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from confluent_kafka import Producer

BASE_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = f"{BASE_PATH}/data/ml"
os.makedirs(DATA_DIR, exist_ok=True)

# Configuración básica del Productor de Kafka local
KAFKA_CONF = {'bootstrap.servers': 'localhost:9092'}

default_args = {
    'owner': 'clase_etl',
    'start_date': days_ago(1),
    'retries': 0,
}

# --- 1. Generar Datos ---
def generate_and_split_data():
    np.random.seed(42)
    
    # 1. Primero creamos el DataFrame con las variables independientes
    df = pd.DataFrame({
        'user_id': range(1, 1001),
        'usage_minutes': np.random.randint(10, 1000, 1000),
        'customer_service_calls': np.random.randint(0, 10, 1000),
        'monthly_charge': np.random.uniform(10.0, 120.0, 1000)
    })
    
    # 2. Luego calculamos el churn_risk usando las columnas de Pandas directamente.
    # Al hacer df['columna'] * float, Pandas hace el cálculo para cada fila automáticamente sin dar error.
    riesgo_calculado = (
        (-0.002 * df['usage_minutes']) + 
        (0.08 * df['customer_service_calls']) + 
        (0.005 * df['monthly_charge'])
    )
    
    # 3. Normalizamos el riesgo para que quede estrictamente entre 0.0 y 1.0
    # (Para evitar que la regresión lineal reciba números negativos o mayores a 1)
    df['churn_risk'] = (riesgo_calculado - riesgo_calculado.min()) / (riesgo_calculado.max() - riesgo_calculado.min())
    
    # 4. Separar y guardar
    train, test = train_test_split(df, test_size=0.2, random_state=42)
    
    # Asegúrate de que DATA_DIR esté definido arriba en tu archivo
    train.to_csv(f"{DATA_DIR}/train.csv", index=False)
    test.to_csv(f"{DATA_DIR}/test.csv", index=False)
    print("Datos generados y separados (80/20).")

# --- 2. Entrenar Regresión Lineal ---
def train_lr_model():
    train = pd.read_csv(f"{DATA_DIR}/train.csv")
    X = train[['usage_minutes', 'customer_service_calls', 'monthly_charge']]
    y = train['churn_risk']
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Evaluar rendimiento en entrenamiento
    y_pred = model.predict(X)
    print(f"Rendimiento TRAIN LR - R2: {r2_score(y, y_pred)}, MAE: {mean_absolute_error(y, y_pred)}")
    
    with open(f"{DATA_DIR}/model_lr.pkl", "wb") as f:
        pickle.dump(model, f)

# --- 3. Entrenar Decision Tree ---
def train_dt_model():
    train = pd.read_csv(f"{DATA_DIR}/train.csv")
    X = train[['usage_minutes', 'customer_service_calls', 'monthly_charge']]
    y = train['churn_risk']
    
    model = DecisionTreeRegressor(max_depth=5, random_state=42)
    model.fit(X, y)
    
    # Evaluar rendimiento en entrenamiento
    y_pred = model.predict(X)
    print(f"Rendimiento TRAIN DT - R2: {r2_score(y, y_pred)}, MAE: {mean_absolute_error(y, y_pred)}")
    
    with open(f"{DATA_DIR}/model_dt.pkl", "wb") as f:
        pickle.dump(model, f)

# --- 4, 5, 6. Productores de Kafka ---
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

def produce_lr():
    p = Producer(KAFKA_CONF)
    with open(f"{DATA_DIR}/model_lr.pkl", "rb") as f:
        model_bytes = f.read()
    p.produce('topic_model_lr', value=model_bytes, callback=delivery_report)
    p.flush()

def produce_dt():
    p = Producer(KAFKA_CONF)
    with open(f"{DATA_DIR}/model_dt.pkl", "rb") as f:
        model_bytes = f.read()
    
    # Serialización como JSON según el requerimiento
    json_payload = json.dumps({
        "model_type": "decision_tree_regressor",
        "model_base64": base64.b64encode(model_bytes).decode('utf-8')
    })
    
    p.produce('topic_model_dt', value=json_payload.encode('utf-8'), callback=delivery_report)
    p.flush()

def produce_test_batch():
    p = Producer(KAFKA_CONF)
    test_df = pd.read_csv(f"{DATA_DIR}/test.csv")
    
    # Enviar datos como JSON
    records = test_df.to_dict(orient='records')
    p.produce('topic_test_batch', value=json.dumps(records).encode('utf-8'), callback=delivery_report)
    p.flush()



s = sched.scheduler(time.time, time.sleep)

def produce_test_stream():
    p = Producer(KAFKA_CONF)
    test_df = pd.read_csv(f"{DATA_DIR}/test.csv")
    print(f"Iniciando streaming de {len(test_df)} registros...")

    # Convertimos el DataFrame a una lista de diccionarios para facilitar el manejo
    records = test_df.to_dict('records')
    
    def send_record(index):
        if index < len(records):
            # Enviar el registro actual
            record = records[index]
            p.produce('topic_test_stream', value=json.dumps(record).encode('utf-8'))
            p.poll(0)
            p.flush()
            
            # Programar el SIGUIENTE registro en 1 segundo
            s.enter(1, 1, send_record, argument=(index + 1,))
        else:
            print("Streaming finalizado.")

    # Programar el primer envío inmediatamente
    s.enter(0, 1, send_record, argument=(0,))
    
    threading.Thread(target=s.run, daemon=True).start()

# --- Definición del Grafo ---
with DAG('churn_ml_kafka_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='generate_data', python_callable=generate_and_split_data)
    t2 = PythonOperator(task_id='train_lr', python_callable=train_lr_model)
    t3 = PythonOperator(task_id='train_dt', python_callable=train_dt_model)
    t4 = PythonOperator(task_id='produce_lr', python_callable=produce_lr)
    t5 = PythonOperator(task_id='produce_dt', python_callable=produce_dt)
    
    t6_batch = PythonOperator(task_id='produce_test_batch', python_callable=produce_test_batch)
    t6_stream = PythonOperator(task_id='produce_test_stream', python_callable=produce_test_stream)

    t1 >> [t2, t3]
    t2 >> t4 >> t6_batch
    t3 >> t5 >> t6_stream
