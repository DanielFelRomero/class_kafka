#!/bin/bash
echo "📥 Descargando Kafka..."
wget -q https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz

echo "⚙️ Configurando el almacenamiento usando KRaft (sin Zookeeper)..."
uuid=$(./kafka_2.13-3.7.0/bin/kafka-storage.sh random-uuid)
./kafka_2.13-3.7.0/bin/kafka-storage.sh format -t $uuid -c ./kafka_2.13-3.7.0/config/kraft/server.properties

echo "🚀 Iniciando el servidor de Kafka en modo daemon..."
./kafka_2.13-3.7.0/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.7.0/config/kraft/server.properties

echo "⏳ Esperando 10 segundos para asegurar que el broker esté arriba..."
sleep 10
echo "✅ ¡Entorno de Kafka listo!"
