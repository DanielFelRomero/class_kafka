import streamlit as st
import pandas as pd
import os

st.title("Monitor de Modelos de Predicción de Churn")

path_lr = '/opt/airflow/data/ml/results_lr.csv'
path_dt = '/opt/airflow/data/ml/results_dt.csv'

col1, col2 = st.columns(2)

with col1:
    st.subheader("Linear Regression")
    if os.path.exists(path_lr):
        df_lr = pd.read_csv(path_lr)
        st.dataframe(df_lr[['user_id', 'churn_risk', 'prediction_lr']].head(10))
        st.line_chart(df_lr[['churn_risk', 'prediction_lr']].head(50)) # Muestra los primeros 50 para claridad
    else:
        st.info("Esperando predicciones de LR desde Kafka...")

with col2:
    st.subheader("Decision Tree")
    if os.path.exists(path_dt):
        df_dt = pd.read_csv(path_dt)
        st.dataframe(df_dt[['user_id', 'churn_risk', 'prediction_dt']].head(10))
        st.line_chart(df_dt[['churn_risk', 'prediction_dt']].head(50))
    else:
        st.info("Esperando predicciones de DT desde Kafka...")
