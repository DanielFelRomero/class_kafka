import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(page_title="Monitor ML Churn", layout="wide")
st.title("Monitor Híbrido: Batch vs Streaming")

BASE_PATH = os.environ.get('AIRFLOW_HOME', '/workspaces/class_kafka')
path_lr = f'{BASE_PATH}/data/ml/results_lr.csv'
path_dt = f'{BASE_PATH}/data/ml/results_dt.csv'

# Contenedor dinámico que se actualizará
placeholder = st.empty()

# Bucle infinito para refrescar la UI (Dashboard Real-Time)
while True:
    with placeholder.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.header("🏢 Regresión Lineal (Procesamiento Batch)")
            st.write("Recibe todos los datos de golpe y predice en bloque.")
            if os.path.exists(path_lr):
                df_lr = pd.read_csv(path_lr)
                st.success(f"Lote procesado: {len(df_lr)} registros.")
                # Muestra una muestra y la gráfica completa
                st.dataframe(df_lr[['user_id', 'churn_risk', 'prediction_lr']].head(10), use_container_width=True)
                st.line_chart(df_lr[['churn_risk', 'prediction_lr']])
            else:
                st.info("⏳ Esperando el lote de datos (Batch)...")

        with col2:
            st.header("⚡ Decision Tree (Procesamiento Streaming)")
            st.write("Recibe un evento por segundo, predice y actualiza.")
            if os.path.exists(path_dt):
                df_dt = pd.read_csv(path_dt)
                
                # Métrica en tiempo real
                st.metric(label="Eventos en Vivo Procesados", value=len(df_dt))
                
                # Muestra los ÚLTIMOS registros en llegar
                st.dataframe(df_dt[['user_id', 'churn_risk', 'prediction_dt']].tail(10), use_container_width=True)
                
                # La gráfica irá creciendo a medida que lleguen los datos
                st.line_chart(df_dt[['churn_risk', 'prediction_dt']])
            else:
                st.info("⏳ Esperando eventos en streaming...")
    
    # Pausa de 1.5 segundos antes de volver a leer el CSV y repintar
    time.sleep(1.5)
