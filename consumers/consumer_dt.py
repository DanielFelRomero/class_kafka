from confluent_kafka import Consumer
import json
import base64
import pickle
import pandas as pd
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group_dt',
    'auto.offset.reset': 'earliest',
    'enabvle.auto.commit': True
}
c = Consumer(conf)
c.subscribe(['topic_model_dt', 'topic_test_data'])

model = None
print("Escuchando modelo DT y datos de test...")

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error(): continue

    if msg.topic() == 'topic_model_dt':
        # Deserialización del JSON
        payload = json.loads(msg.value().decode('utf-8'))
        model_bytes = base64.b64decode(payload['model_base64'])
        model = pickle.loads(model_bytes)
        print("Modelo DT recibido vía JSON.")
        
    elif msg.topic() == 'topic_test_data' and model is not None:
        test_data = json.loads(msg.value().decode('utf-8'))
        df = pd.DataFrame(test_data)
        
        X_test = df[['usage_minutes', 'customer_service_calls', 'monthly_charge']]
        y_test = df['churn_risk']
        predictions = model.predict(X_test)
        
        print("\n--- MÉTRICAS TEST DECISION TREE ---")
        print(f"R2: {r2_score(y_test, predictions):.4f}")
        print(f"MAE: {mean_absolute_error(y_test, predictions):.4f}")
        print(f"RMSE: {mean_squared_error(y_test, predictions, squared=False):.4f}")
        
        # Guardar para Streamlit
        df['prediction_dt'] = predictions
        df.to_csv('/opt/airflow/data/ml/results_dt.csv', index=False)
