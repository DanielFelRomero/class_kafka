from confluent_kafka import Consumer
import json
import base64
import pickle
import pandas as pd
import os

BASE_PATH = os.environ.get('AIRFLOW_HOME', '/workspaces/class_kafka')
DATA_DIR = f"{BASE_PATH}/data/ml"
os.makedirs(DATA_DIR, exist_ok=True)

path_dt = f'{DATA_DIR}/results_dt.csv'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group_dt_stream',
    'auto.offset.reset': 'earliest'
}
c = Consumer(conf)
c.subscribe(['topic_model_dt', 'topic_test_stream'])

model = None

# Limpiar resultados anteriores para el demo
if os.path.exists(path_dt):
    os.remove(path_dt)

print("Escuchando modelo DT y eventos de streaming...")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None: continue
        if msg.error(): continue
    
        if msg.topic() == 'topic_model_dt':
            payload = json.loads(msg.value().decode('utf-8'))
            model = pickle.loads(base64.b64decode(payload['model_base64']))
            print("✅ Modelo Decision Tree cargado.")
            
        elif msg.topic() == 'topic_test_stream' and model is not None:
            # 1. Recibe UN SOLO registro en streaming
            record = json.loads(msg.value().decode('utf-8'))
            df_single = pd.DataFrame([record])
            
            # 2. Predicción en tiempo real
            X_test = df_single[['usage_minutes', 'customer_service_calls', 'monthly_charge']]
            pred = model.predict(X_test)[0]
            df_single['prediction_dt'] = pred
            
            # 3. Anexar (Append) a la "base de datos"
            if not os.path.isfile(path_dt):
                df_single.to_csv(path_dt, index=False) # Crea el archivo con cabeceras
            else:
                df_single.to_csv(path_dt, mode='a', header=False, index=False) # Agrega la fila
                
            print(f" Evento procesado -> UserID: {record['user_id']} | Predicción Churn: {pred:.2f}")

except KeyboardInterrupt:
    print("Deteniendo consumer...")
finally:
    print("Cerrando consumer_dt...")
    c.close()
