FROM eclipse-temurin:21-jdk-noble

LABEL maintainer="Prof. Barbosa <infobarbosa@gmail.com>"
LABEL description="Apache Spark 4.0.3 - Cluster Lab"

# ============================================
# Argumento de build
# ============================================
ARG SPARK_VERSION=4.0.3
ARG HADOOP_VERSION=hadoop3

# ============================================
# Variáveis de ambiente
# ============================================
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_NO_DAEMONIZE=true
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# ============================================
# Dependências do sistema
# ============================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    tini \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ============================================
# Usuário spark (não-root)
# ============================================
RUN groupadd -r spark \
    && useradd -r -g spark -m -d /home/spark spark

# ============================================
# Download e instalação do Apache Spark
# ============================================
RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} ${SPARK_HOME} \
    && chown -R spark:spark ${SPARK_HOME}

# ============================================
# Diretórios auxiliares
# ============================================
RUN mkdir -p /tmp/spark-events \
    && chown spark:spark /tmp/spark-events

# ============================================
# Arquivos de configuração
# ============================================
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh \
    && chown spark:spark /opt/entrypoint.sh \
    && chown spark:spark ${SPARK_HOME}/conf/spark-defaults.conf

# ============================================
# Configuração final
# ============================================
WORKDIR ${SPARK_HOME}
USER spark

ENTRYPOINT ["tini", "--"]
CMD ["/opt/entrypoint.sh"]
