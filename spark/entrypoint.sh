#!/bin/bash
set -e

# ============================================
# Variáveis com valores padrão
# ============================================
SPARK_MODE=${SPARK_MODE:-master}
SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}
SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}
SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-4g}
SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}

echo "============================================"
echo " Apache Spark - Modo: ${SPARK_MODE}"
echo "============================================"

if [ "$SPARK_MODE" = "master" ]; then
    # ------------------------------------------
    # Iniciar como MASTER
    # ------------------------------------------
    echo "Iniciando Spark Master..."

    exec "${SPARK_HOME}/bin/spark-class" \
        org.apache.spark.deploy.master.Master \
        --host "$(hostname)" \
        --port 7077 \
        --webui-port 8080

elif [ "$SPARK_MODE" = "worker" ]; then
    # ------------------------------------------
    # Iniciar como WORKER
    # ------------------------------------------

    # Aguardar o Master ficar disponível
    echo "Aguardando Spark Master em ${SPARK_MASTER_URL}..."
    while ! (echo > /dev/tcp/spark-master/7077) 2>/dev/null; do
        echo "  Master ainda não disponível. Tentando em 2s..."
        sleep 2
    done
    echo "Spark Master disponível!"

    echo "Iniciando Worker com ${SPARK_WORKER_CORES} cores e ${SPARK_WORKER_MEMORY} de memória..."

    exec "${SPARK_HOME}/bin/spark-class" \
        org.apache.spark.deploy.worker.Worker \
        --host "$(hostname)" \
        --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
        --cores "${SPARK_WORKER_CORES}" \
        --memory "${SPARK_WORKER_MEMORY}" \
        "${SPARK_MASTER_URL}"

else
    echo "ERRO: SPARK_MODE deve ser 'master' ou 'worker'. Valor recebido: '${SPARK_MODE}'"
    exit 1
fi
