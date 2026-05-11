from confluent_kafka import Consumer
import pickle
import json
import pandas as pd
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

BASE_PATH = os.environ.get('AIRFLOW_HOME', '/workspaces/class_kafka')
DATA_DIR = f"{BASE_PATH}/data/ml"
os.makedirs(DATA_DIR, exist_ok=True)

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group_lr',
    'auto.offset.reset': 'earliest'
}
c = Consumer(conf)
c.subscribe(['topic_model_lr', 'topic_test_batch'])

model = None
print("Escuchando modelo LR y datos de test...")

try:
    while True:
        msg = c.poll(timeout = 1.0)
        if msg is None: continue
        if msg.error(): continue

        if msg.topic() == 'topic_model_lr':
            model = pickle.loads(msg.value())
            print("Modelo LR recibido vía Pickle.")
            
        elif msg.topic() == 'topic_test_batch' and model is not None:
            test_data = json.loads(msg.value().decode('utf-8'))
            df = pd.DataFrame(test_data)
            
            X_test = df[['usage_minutes', 'customer_service_calls', 'monthly_charge']]
            y_test = df['churn_risk']
            predictions = model.predict(X_test)
            
            print("\n--- MÉTRICAS TEST LINEAR REGRESSION ---")
            print(f"R2: {r2_score(y_test, predictions):.4f}")
            print(f"MAE: {mean_absolute_error(y_test, predictions):.4f}")
            print(f"RMSE: {mean_squared_error(y_test, predictions, squared=False):.4f}")
            
            # Guardar para Streamlit
            df['prediction_lr'] = predictions
            df.to_csv('/opt/airflow/data/ml/results_lr.csv', index=False)

            print("\nProcesamiento terminado.")

            break

except KeyboardInterrupt:
    print("\nInterrumpido por el usuario.")
finally:
    print("Cerrando consumer...")
    c.close()
