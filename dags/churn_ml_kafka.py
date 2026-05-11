from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import pickle
import json
import base64
import os
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
    # Generamos un dataset aleatorio donde churn_risk es continuo (Regresión)
    usage_minutes = np.random.randint(10, 1000, 1000),
    customer_service_calls = np.random.randint(0, 10, 1000),
    monthly_charge = np.random.uniform(10.0, 120.0, 1000)

    # Construcción del logit
    logit = (
        -0.002 * usage_minutes
        + 0.35 * customer_service_calls
        + 0.025 * monthly_charge
        + np.random.normal(0, 1.0, n)
    )

    churn_risk = 1 / (1 + np.exp(-logit))
    
    df = pd.DataFrame({
        'user_id': range(1, 1001),
        'usage_minutes': usage_minutes,
        'customer_service_calls': customer_service_calls,
        'monthly_charge': monthly_charge,
        'churn_risk': churn_risk
    })
    
    train, test = train_test_split(df, test_size=0.2, random_state=42)
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

def produce_test_data():
    p = Producer(KAFKA_CONF)
    test_df = pd.read_csv(f"{DATA_DIR}/test.csv")
    
    # Enviar datos como JSON
    records = test_df.to_dict(orient='records')
    p.produce('topic_test_data', value=json.dumps(records).encode('utf-8'), callback=delivery_report)
    p.flush()

# --- Definición del Grafo ---
with DAG('churn_ml_kafka_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='generate_data', python_callable=generate_and_split_data)
    t2 = PythonOperator(task_id='train_lr', python_callable=train_lr_model)
    t3 = PythonOperator(task_id='train_dt', python_callable=train_dt_model)
    t4 = PythonOperator(task_id='produce_lr', python_callable=produce_lr)
    t5 = PythonOperator(task_id='produce_dt', python_callable=produce_dt)
    t6 = PythonOperator(task_id='produce_test', python_callable=produce_test_data)

    t1 >> [t2, t3]
    t2 >> t4
    t3 >> t5
    [t4, t5] >> t6
