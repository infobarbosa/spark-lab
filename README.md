# Apache Spark Cluster Lab

- **Author**: Prof. Barbosa
- **Contact**: infobarbosa@gmail.com
- **Github**: [infobarbosa](https://github.com/infobarbosa)

---

## 1. Objetivo

Neste laboratório, você vai construir **do zero** um cluster Apache Spark em modo **standalone** usando Docker.

Ao final, você terá:
- Um cluster Spark com **1 Master** e **2 Workers**
- Um cluster **Apache Ozone** para armazenamento distribuído
- Todas as interfaces web acessíveis no seu navegador
- Um template pronto para submeter jobs PySpark

---

## 2. Arquitetura do Cluster

O cluster é composto por **6 containers** Docker, todos na mesma rede bridge com IPs fixos:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Docker Bridge: spark-net                         │
│                    Subnet: 172.30.0.0/24                            │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │ spark-master │  │spark-worker-1│  │spark-worker-2│               │
│  │ 172.30.0.10  │  │ 172.30.0.11  │  │ 172.30.0.12  │               │
│  │ :8080 (UI)   │  │ :8081 (UI)   │  │ :8082 (UI)   │               │
│  │ :7077 (RPC)  │  │              │  │              │               │
│  │ :4040 (App)  │  │              │  │              │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │  ozone-scm   │  │   ozone-om   │  │ozone-datanode│               │
│  │ 172.30.0.20  │  │ 172.30.0.21  │  │ 172.30.0.22  │               │
│  │ :9876 (UI)   │  │ :9874 (UI)   │  │ :9882 (UI)   │               │
│  └──────────────┘  └──────────────┘  └──────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

### Tabela de Serviços

| Serviço | IP | Porta | Descrição |
|---------|-----|-------|-----------|
| spark-master | 172.30.0.10 | 8080 | Spark Master Web UI |
| spark-master | 172.30.0.10 | 7077 | Spark Master RPC |
| spark-master | 172.30.0.10 | 4040 | Spark Application UI |
| spark-worker-1 | 172.30.0.11 | 8081 | Worker 1 Web UI |
| spark-worker-2 | 172.30.0.12 | 8082 | Worker 2 Web UI |
| ozone-scm | 172.30.0.20 | 9876 | Storage Container Manager UI |
| ozone-om | 172.30.0.21 | 9874 | Ozone Manager UI |
| ozone-datanode | 172.30.0.22 | 9882 | DataNode Web UI |

### Versões

| Componente | Versão |
|-----------|--------|
| Apache Spark | 4.0.3 |
| Java | 21 (Eclipse Temurin) |
| Apache Ozone | 2.1.0 |
| Imagem Base (Spark) | `eclipse-temurin:21-jdk-noble` |
| Imagem Base (Ozone) | `apache/ozone:2.1.0` |

---

## 3. Pré-requisitos

Antes de começar, verifique se você tem o Docker e o Docker Compose V2 instalados.

**Docker Engine:**
```bash
docker --version
```

Saída esperada (versão 24 ou superior):
```
Docker version 27.x.x, build xxxxxxx
```

**Docker Compose V2:**
```bash
docker compose version
```

Saída esperada:
```
Docker Compose version v2.x.x
```

> **Nota:** Usamos `docker compose` (sem hífen), que é o Docker Compose V2 integrado como plugin do Docker.

---

## 4. Estrutura do Projeto

Vamos criar a estrutura de diretórios do projeto:

```bash
mkdir -p spark apps data

```

Verifique a estrutura criada:

```bash
find . -type d | head -20
```

Ao final deste laboratório, a estrutura completa será:

```
spark-lab/
├── .env                         # Variáveis de ambiente
├── compose.yml                  # Orquestração dos serviços
├── spark/
│   ├── Dockerfile               # Imagem customizada do Spark
│   ├── entrypoint.sh            # Script de inicialização
│   └── spark-defaults.conf      # Configuração do Spark
├── apps/
│   └── example-job.py           # Template para jobs PySpark
└── data/                        # Dados para processamento
```

---

## 5. Arquivo de Variáveis de Ambiente (.env)

O arquivo `.env` centraliza as configurações que o Docker Compose vai utilizar. Isso facilita a manutenção e evita valores hardcoded no `compose.yml`.

Crie o arquivo:

```bash
cat <<'EOF' > .env
# ============================================
# Apache Spark Cluster Lab - Variáveis
# ============================================

# Versões
SPARK_VERSION=4.0.3
OZONE_VERSION=2.1.0

# Spark - Recursos por Worker
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=4g

# Spark - Recursos do Driver/Executor (usado no spark-submit)
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
EOF
```

Verifique o conteúdo:

```bash
cat .env
```

**Entendendo as variáveis:**
- `SPARK_VERSION` / `OZONE_VERSION`: Versões dos componentes. Altere aqui para atualizar todo o cluster.
- `SPARK_WORKER_CORES=2`: Cada worker usa 2 cores. Com 2 workers, são **4 cores** no total.
- `SPARK_WORKER_MEMORY=4g`: Cada worker disponibiliza 4 GB para executors. Total: **8 GB**.
- `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY`: Memória usada ao submeter jobs.

> **Nota:** Com 2 workers × 4 GB = 8 GB para o Spark, sobram ~24 GB para o sistema operacional, Ozone e overhead do Docker em uma máquina de 32 GB.

---

## 6. O Dockerfile do Spark

Agora vamos construir a imagem Docker do Apache Spark **do zero**. Isso é mais didático do que usar imagens prontas porque você entende exatamente o que compõe a imagem.

A imagem será baseada em `eclipse-temurin:21-jdk-noble`, que é o Ubuntu 24.04 com o Java 21 já instalado.

### 6.1 Baixando o Apache Spark

Primeiro, baixe o arquivo do Apache Spark para dentro do diretório `spark/`:

```bash
wget -q --show-progress -O spark/spark-4.0.3-bin-hadoop3.tgz \
    "https://downloads.apache.org/spark/spark-4.0.3/spark-4.0.3-bin-hadoop3.tgz"
```

> **Nota:** O arquivo tem ~524 MB. O download pode levar alguns minutos dependendo da sua conexão.

Verifique o arquivo:

```bash
ls -lh spark/spark-4.0.3-bin-hadoop3.tgz
```

### 6.2 Criando o Dockerfile

Crie o arquivo:

```bash
cat <<'EOF' > spark/Dockerfile
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
# Instalação do Apache Spark
# (o .tgz deve ser baixado previamente no host)
# ============================================
COPY spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} ${SPARK_HOME} \
    && chown -R spark:spark ${SPARK_HOME} \
    && rm /tmp/spark.tgz

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
EOF
```

**Entendendo o Dockerfile:**

| Instrução | O que faz |
|-----------|-----------|
| `FROM eclipse-temurin:21-jdk-noble` | Usa Ubuntu 24.04 com Java 21 como base |
| `ARG SPARK_VERSION` | Permite mudar a versão do Spark no build sem editar o arquivo |
| `ENV SPARK_NO_DAEMONIZE=true` | Força o Spark a rodar em foreground (necessário para Docker) |
| `ENV PIP_BREAK_SYSTEM_PACKAGES=1` | Permite instalar pacotes Python com pip no Ubuntu 24.04 |
| `tini` | Gerenciador de processos leve; trata sinais (SIGTERM) corretamente |
| `python3` + `python3-pip` | Necessários para rodar jobs PySpark |
| `groupadd` / `useradd` | Cria usuário não-root (boa prática de segurança) |
| `COPY spark-*.tgz` | Copia o Spark pré-baixado para dentro da imagem (mais rápido que download no build) |
| `USER spark` | O container roda como usuário `spark`, não como root |
| `ENTRYPOINT ["tini", "--"]` | Tini como PID 1; o CMD define o processo principal |

---

## 7. O Script de Entrypoint

O script de entrypoint é o "cérebro" do container. Ele decide se o container deve iniciar como **Master** ou como **Worker** baseado na variável de ambiente `SPARK_MODE`.

Crie o arquivo:

```bash
cat <<'EOF' > spark/entrypoint.sh
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
EOF
```

Torne o script executável:

```bash
chmod +x spark/entrypoint.sh
```

**Entendendo o script:**

- `set -e`: O script para imediatamente se qualquer comando falhar.
- `SPARK_MODE`: Variável que define o papel do container (`master` ou `worker`).
- `/dev/tcp/spark-master/7077`: Recurso nativo do Bash para testar se uma porta TCP está aberta. O worker fica em loop até o Master aceitar conexões.
- `exec`: Substitui o processo do shell pelo processo do Spark. Isso garante que o Spark seja o processo principal (PID 1 via tini) e receba sinais do Docker corretamente.
- `spark-class`: Comando interno do Spark que inicia uma classe Java específica (Master ou Worker).

---

## 8. Configuração do Spark

O arquivo `spark-defaults.conf` define as configurações padrão do Spark. Essas propriedades são aplicadas automaticamente em todos os jobs.

Crie o arquivo:

```bash
cat <<'EOF' > spark/spark-defaults.conf
# ============================================
# Apache Spark - Configuração Padrão
# ============================================

# Endereço do Master
spark.master                        spark://spark-master:7077

# Event Log (histórico de jobs)
spark.eventLog.enabled              true
spark.eventLog.dir                  /tmp/spark-events
spark.history.fs.logDirectory       /tmp/spark-events

# Reverse Proxy (permite navegar nas UIs dos Workers via Master UI)
spark.ui.reverseProxy               true
EOF
```

**Entendendo as propriedades:**

| Propriedade | Descrição |
|-------------|-----------|
| `spark.master` | URL do Master. Os Workers e jobs usam esse endereço para se conectar. |
| `spark.eventLog.enabled` | Habilita o registro de eventos dos jobs para análise posterior. |
| `spark.eventLog.dir` | Diretório onde os event logs são salvos. |
| `spark.ui.reverseProxy` | Permite acessar as UIs dos Workers **através** da UI do Master. Sem isso, os links apontariam para hostnames internos do Docker (inacessíveis de fora). |

---

## 9. Apache Ozone

O **Apache Ozone** é um sistema de armazenamento distribuído compatível com a API S3 e o protocolo Hadoop. Ele é composto por três componentes principais:

- **SCM** (Storage Container Manager): Gerencia os containers de armazenamento e os DataNodes.
- **OM** (Ozone Manager): Gerencia o namespace (volumes, buckets, keys).
- **DataNode**: Armazena os dados propriamente ditos.

Neste lab, vamos usar o setup mínimo: **1 SCM + 1 OM + 1 DataNode**.

A configuração do Ozone é feita diretamente via **variáveis de ambiente** no `compose.yml`, usando o prefixo `OZONE-SITE.XML_`. O entrypoint da imagem oficial `apache/ozone` converte automaticamente essas variáveis em propriedades no `ozone-site.xml` dentro de cada container durante a inicialização.

> **Nota:** Não é necessário criar um arquivo `ozone-site.xml` manualmente. Todas as propriedades são definidas na seção `environment` de cada serviço Ozone no `compose.yml`.

As principais propriedades configuradas são:

| Propriedade | Valor | Descrição |
|-------------|-------|-----------|
| `ozone.om.address` | `ozone-om` | Hostname do Ozone Manager (nome do container Docker) |
| `ozone.scm.names` | `ozone-scm` | Hostname do SCM |
| `ozone.scm.client.address` | `ozone-scm` | Endereço do cliente SCM |
| `ozone.scm.block.client.address` | `ozone-scm` | Endereço do cliente de blocos SCM |
| `ozone.metadata.dirs` | `/data/metadata` | Diretório de metadados |
| `ozone.scm.datanode.id.dir` | `/data/metadata/datanode` | Diretório de IDs do DataNode |
| `hdds.datanode.dir` | `/data/hdds` | Diretório de dados do DataNode |
| `hdds.scm.safemode.min.datanode` | `1` | Mínimo de DataNodes para sair do safe mode |
| `ozone.replication` | `ONE` | Fator de replicação (`ONE` porque temos apenas 1 DataNode) |

---

## 10. Docker Compose

O `compose.yml` é o arquivo central que orquestra todos os containers. Ele define:
- Os 3 serviços do Spark (1 master + 2 workers)
- Os 3 serviços do Ozone (SCM + OM + DataNode)
- A rede bridge com IPs fixos
- Os volumes para persistência de dados

Crie o arquivo:

```bash
cat <<'EOF' > compose.yml
# ============================================
# Apache Spark Cluster Lab
# Docker Compose - Todos os serviços
# ============================================

services:

  # ------------------------------------------
  # SPARK MASTER
  # ------------------------------------------
  spark-master:
    build:
      context: ./spark
      args:
        SPARK_VERSION: ${SPARK_VERSION:-4.0.3}
    image: spark-lab:${SPARK_VERSION:-4.0.3}
    container_name: spark-master
    hostname: spark-master
    environment:
      SPARK_MODE: master
    ports:
      - "8080:8080"    # Master Web UI
      - "7077:7077"    # Master RPC
      - "4040:4040"    # Application Web UI
    volumes:
      - spark-events:/tmp/spark-events
      - ./apps:/apps
      - ./data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.10
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 30s

  # ------------------------------------------
  # SPARK WORKER 1
  # ------------------------------------------
  spark-worker-1:
    image: spark-lab:${SPARK_VERSION:-4.0.3}
    container_name: spark-worker-1
    hostname: spark-worker-1
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-2}
      SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-4g}
      SPARK_WORKER_WEBUI_PORT: 8081
    ports:
      - "8081:8081"    # Worker 1 Web UI
    volumes:
      - spark-events:/tmp/spark-events
      - ./data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.11
    depends_on:
      spark-master:
        condition: service_healthy

  # ------------------------------------------
  # SPARK WORKER 2
  # ------------------------------------------
  spark-worker-2:
    image: spark-lab:${SPARK_VERSION:-4.0.3}
    container_name: spark-worker-2
    hostname: spark-worker-2
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-2}
      SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-4g}
      SPARK_WORKER_WEBUI_PORT: 8082
    ports:
      - "8082:8082"    # Worker 2 Web UI
    volumes:
      - spark-events:/tmp/spark-events
      - ./data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.12
    depends_on:
      spark-master:
        condition: service_healthy

# ------------------------------------------
  # OZONE - Storage Container Manager (SCM)
  # ------------------------------------------
  ozone-scm:
    image: apache/ozone:${OZONE_VERSION:-2.1.0}
    container_name: ozone-scm
    hostname: ozone-scm
    environment:
      OZONE-SITE.XML_ozone.om.address: ozone-om
      OZONE-SITE.XML_ozone.scm.names: ozone-scm
      OZONE-SITE.XML_ozone.scm.client.address: ozone-scm
      OZONE-SITE.XML_ozone.scm.block.client.address: ozone-scm
      OZONE-SITE.XML_ozone.metadata.dirs: /data/metadata
      OZONE-SITE.XML_ozone.scm.datanode.id.dir: /data/metadata/datanode
      OZONE-SITE.XML_hdds.datanode.dir: /data/hdds
      OZONE-SITE.XML_hdds.scm.safemode.min.datanode: "1"
      OZONE-SITE.XML_ozone.replication: ONE
    command: >
      bash -c "
        if [ ! -d /data/metadata/scm/current ]; then
          echo 'Inicializando SCM...'
          ozone scm --init
        fi
        exec ozone scm
      "
    ports:
      - "9876:9876"    # SCM Web UI
    volumes:
      - ozone-scm-data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.20
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9876 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 40s

# ------------------------------------------
  # OZONE - Ozone Manager (OM)
  # ------------------------------------------
  ozone-om:
    image: apache/ozone:${OZONE_VERSION:-2.1.0}
    container_name: ozone-om
    hostname: ozone-om
    environment:
      OZONE-SITE.XML_ozone.om.address: ozone-om
      OZONE-SITE.XML_ozone.scm.names: ozone-scm
      OZONE-SITE.XML_ozone.scm.client.address: ozone-scm
      OZONE-SITE.XML_ozone.scm.block.client.address: ozone-scm
      OZONE-SITE.XML_ozone.metadata.dirs: /data/metadata
      OZONE-SITE.XML_ozone.scm.datanode.id.dir: /data/metadata/datanode
      OZONE-SITE.XML_hdds.datanode.dir: /data/hdds
      OZONE-SITE.XML_hdds.scm.safemode.min.datanode: "1"
      OZONE-SITE.XML_ozone.replication: ONE
    command: >
      bash -c "
        if [ ! -d /data/metadata/om/current ]; then
          echo 'Inicializando OM...'
          ozone om --init
        fi
        exec ozone om
      "
    ports:
      - "9874:9874"    # OM Web UI
    volumes:
      - ozone-om-data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.21
    depends_on:
      ozone-scm:
        condition: service_healthy

# ------------------------------------------
  # OZONE - DataNode
  # ------------------------------------------
  ozone-datanode:
    image: apache/ozone:${OZONE_VERSION:-2.1.0}
    container_name: ozone-datanode
    hostname: ozone-datanode
    environment:
      OZONE-SITE.XML_ozone.om.address: ozone-om
      OZONE-SITE.XML_ozone.scm.names: ozone-scm
      OZONE-SITE.XML_ozone.scm.client.address: ozone-scm
      OZONE-SITE.XML_ozone.scm.block.client.address: ozone-scm
      OZONE-SITE.XML_ozone.metadata.dirs: /data/metadata
      OZONE-SITE.XML_ozone.scm.datanode.id.dir: /data/metadata/datanode
      OZONE-SITE.XML_hdds.datanode.dir: /data/hdds
      OZONE-SITE.XML_hdds.scm.safemode.min.datanode: "1"
      OZONE-SITE.XML_ozone.replication: ONE
    command: ["ozone", "datanode"]
    ports:
      - "9882:9882"    # DataNode Web UI
    volumes:
      - ozone-dn-data:/data
    networks:
      spark-net:
        ipv4_address: 172.30.0.22
    depends_on:
      ozone-scm:
        condition: service_healthy

# ============================================
# Rede
# ============================================
networks:
  spark-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.30.0.0/24
          gateway: 172.30.0.1

# ============================================
# Volumes
# ============================================
volumes:
  spark-events:
  ozone-scm-data:
  ozone-om-data:
  ozone-dn-data:
EOF
```

**Pontos importantes do Docker Compose:**

1. **`build` + `image`**: O `spark-master` faz o build do Dockerfile e publica a imagem como `spark-lab:4.0.3`. Os workers **reutilizam** essa mesma imagem sem precisar rebuildar.

2. **`depends_on` com `condition`**: Garante a ordem de inicialização:
   - Workers só iniciam após o Master estar **saudável** (healthcheck passando)
   - OM e DataNode só iniciam após o SCM estar **saudável**

3. **`healthcheck`**: O Master precisa responder na porta 8080 para ser considerado saudável. Isso evita que workers tentem se conectar a um Master que ainda está subindo.

4. **IPs fixos**: Cada container tem um IP fixo na subnet `172.30.0.0/24`. Isso garante previsibilidade e facilita o troubleshooting.

5. **Volumes nomeados**: Os dados do Ozone e os event logs do Spark persistem entre reinicializações do cluster.

---

## 11. Build da Imagem Spark

Com todos os arquivos criados, vamos construir a imagem Docker do Spark:

```bash
docker compose build
```

> **Nota:** O download do Apache Spark (~400 MB) acontece durante o build. Isso pode levar alguns minutos dependendo da sua conexão.

Verifique se a imagem foi criada:

```bash
docker images | grep spark-lab
```

Saída esperada:
```
spark-lab   4.0.3   xxxxxxxxxxxx   X seconds ago   XXX MB
```

---

## 12. Subindo o Cluster

Agora vamos iniciar todo o cluster:

```bash
docker compose up -d
```

O Docker Compose vai:
1. Baixar a imagem do Apache Ozone (se ainda não tiver)
2. Criar a rede `spark-net`
3. Iniciar o SCM do Ozone (e inicializá-lo na primeira vez)
4. Aguardar o SCM ficar saudável, depois iniciar o OM e DataNode
5. Iniciar o Spark Master
6. Aguardar o Master ficar saudável, depois iniciar os 2 Workers

Acompanhe o progresso:

```bash
docker compose ps
```

Aguarde até que todos os containers estejam com status `Up` ou `Up (healthy)`.

---

## 13. Verificação do Cluster

### 13.1 Status dos containers

```bash
docker compose ps
```

Todos os 6 containers devem estar rodando:

```
NAME              STATUS
ozone-datanode    Up
ozone-om          Up
ozone-scm         Up (healthy)
spark-master      Up (healthy)
spark-worker-1    Up
spark-worker-2    Up
```

### 13.2 Logs do Spark Master

Verifique se o Master iniciou corretamente:

```bash
docker compose logs spark-master 2>&1 | head -30
```

Procure pela linha que confirma o Master rodando:
```
Spark Master - Modo: master
Iniciando Spark Master...
```

### 13.3 Workers registrados

Verifique se os 2 workers se registraram no Master:

```bash
docker compose logs spark-master 2>&1 | grep -i "registering worker"
```

Você deve ver 2 linhas, uma para cada worker.

### 13.4 Logs de um Worker

```bash
docker compose logs spark-worker-1 2>&1 | head -20
```

### 13.5 Logs do Ozone

```bash
docker compose logs ozone-scm 2>&1 | head -20
```

```bash
docker compose logs ozone-om 2>&1 | head -20
```

---

## 14. Interfaces Web (UIs)

Acesse as interfaces web pelo navegador. Substitua `<IP_DO_HOST>` pelo IP da sua máquina Linux.

> **Exemplo:** Se o IP do seu Linux é `192.168.0.233`, acesse `http://192.168.0.233:8080`.

Para verificar o IP do host:

```bash
hostname -I | awk '{print $1}'
```

### Spark

| Interface | URL |
|-----------|-----|
| **Spark Master UI** | `http://<IP_DO_HOST>:8080` |
| **Spark Application UI** | `http://<IP_DO_HOST>:4040` (disponível quando há job rodando) |
| **Worker 1 UI** | `http://<IP_DO_HOST>:8081` |
| **Worker 2 UI** | `http://<IP_DO_HOST>:8082` |

Na **Spark Master UI** (porta 8080), você deve ver:
- **Workers**: 2 workers ativos
- **Cores**: 4 cores no total (2 workers × 2 cores)
- **Memory**: 8.0 GB no total (2 workers × 4 GB)

> **Dica:** Com `spark.ui.reverseProxy=true` habilitado, você pode navegar para a UI de cada Worker clicando diretamente nos links da Master UI.

### Apache Ozone

| Interface | URL |
|-----------|-----|
| **Ozone SCM UI** | `http://<IP_DO_HOST>:9876` |
| **Ozone Manager UI** | `http://<IP_DO_HOST>:9874` |
| **DataNode UI** | `http://<IP_DO_HOST>:9882` |

---

## 15. Testando o Apache Ozone

Vamos verificar se o Ozone está funcionando criando um volume, um bucket e fazendo upload de um arquivo.

### 15.1 Criar um volume

```bash
docker compose exec ozone-om ozone sh volume create /test-volume
```

### 15.2 Criar um bucket

```bash
docker compose exec ozone-om ozone sh bucket create /test-volume/test-bucket
```

### 15.3 Listar volumes e buckets

```bash
docker compose exec ozone-om ozone sh volume list /
```

```bash
docker compose exec ozone-om ozone sh bucket list /test-volume
```

### 15.4 Upload de um arquivo de teste

Crie um arquivo de teste local:

```bash
echo "Olá, Apache Ozone! Teste do Spark Cluster Lab." > /tmp/ozone-test.txt
```

Copie para dentro do container e faça o upload:

```bash
docker cp /tmp/ozone-test.txt ozone-om:/tmp/ozone-test.txt
```

```bash
docker compose exec ozone-om ozone sh key put /test-volume/test-bucket/test-key /tmp/ozone-test.txt
```

### 15.5 Download e verificação

```bash
docker compose exec ozone-om ozone sh key get /test-volume/test-bucket/test-key /tmp/ozone-download.txt
```

```bash
docker compose exec ozone-om cat /tmp/ozone-download.txt
```

Se o output for `Olá, Apache Ozone! Teste do Spark Cluster Lab.`, o Ozone está funcionando corretamente! 🎉

### 15.6 Limpar o teste

```bash
docker compose exec ozone-om ozone sh key delete /test-volume/test-bucket/test-key
docker compose exec ozone-om ozone sh bucket delete /test-volume/test-bucket
docker compose exec ozone-om ozone sh volume delete /test-volume
```

---

## 16. Baixando a Base de Dados (Camada Raw)

A base do **Bolsa Família** de Abril/2026 está disponível no [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/202604).

Baixe o arquivo `.zip` e mova-o para a pasta `data/`. Em seguida, descompacte-o:

```bash
cd data/
unzip 202604_NovoBolsaFamilia.zip
cd ..
```

Verifique os arquivos descompactados:

```bash
ls -lh data/
```

---

## 17. Seu Primeiro Job (Template)

Nesta seção, você vai processar a base do **Bolsa Família de Abril/2026** com o Apache Spark integrado ao Apache Ozone. O objetivo é calcular o **total de pagamentos por UF (estado)**.

Ao final, você terá:
- Os dados brutos armazenados no Ozone (bucket `raw`)
- Um job PySpark que lê do Ozone, transforma e agrega os dados
- O resultado gravado de volta no Ozone (bucket `output`)

---

### 17.1 Configurando os buckets no Ozone

O Ozone organiza os dados em **volumes** (semelhante a um namespace) e **buckets** (semelhante a um diretório raiz). Vamos criar a estrutura necessária para o exercício.

**Criar o volume:**

```bash
docker compose exec ozone-om ozone sh volume create /lab
```

**Criar o bucket para dados brutos:**

```bash
docker compose exec ozone-om ozone sh bucket create --replication ONE --type RATIS /lab/raw
```

**Criar o bucket para o resultado do processamento:**

```bash
docker compose exec ozone-om ozone sh bucket create --replication ONE --type RATIS /lab/output
```

**Verificar a estrutura criada:**

```bash
docker compose exec ozone-om ozone sh volume list /
```

```bash
docker compose exec ozone-om ozone sh bucket list /lab
```

---

### 17.2 Baixando a base de dados

> **Nota:** Caso você tenha seguido a Seção 16, o arquivo `.zip` já está disponível em `data/`. Você pode pular para a Seção 17.3.

A base do **Bolsa Família** de Abril/2026 está disponível no [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/202604). Baixe o arquivo `.zip` diretamente para a pasta `data/`:

```bash
wget -q --show-progress \
    -O data/202604_NovoBolsaFamilia.zip \
    "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/202604"
```

> **Nota:** O arquivo comprimido tem aproximadamente 350 MB.

---

### 17.3 Descompactando o arquivo

> **Nota:** Caso você tenha seguido a Seção 16, este passo já foi realizado.

```bash
cd data/
unzip 202604_NovoBolsaFamilia.zip
cd ..
```

Verifique o arquivo extraído e inspecione as primeiras linhas:

```bash
ls -lh data/*.csv
```

```bash
head -3 data/202604_NovoBolsaFamilia.csv
```

Observe que:
- O separador de campos é `;` (ponto-e-vírgula)
- O campo `VALOR PARCELA` usa `,` como separador decimal (ex.: `"800,00"`)
- O arquivo está codificado em `ISO-8859-1` (Latin-1)

---

### 17.4 Copiando o arquivo para o container do Ozone

O container `ozone-om` é quem executa o cliente Ozone (`ozone sh`). Para carregar o CSV no Ozone, precisamos primeiro copiá-lo para dentro desse container via `docker cp`:

```bash
docker cp data/202604_NovoBolsaFamilia.csv \
    ozone-om:/tmp/202604_NovoBolsaFamilia.csv
```

> **Nota:** O arquivo CSV tem aproximadamente 2 GB. A cópia pode levar alguns minutos.

---

### 17.5 Carregando a base de dados no Ozone

Com o arquivo disponível dentro do container, faça o upload para o bucket `raw`:

```bash
docker compose exec ozone-om ozone sh key put \
    /lab/raw/202604_NovoBolsaFamilia.csv \
    /tmp/202604_NovoBolsaFamilia.csv
```

Verifique se o arquivo foi carregado com sucesso:

```bash
docker compose exec ozone-om ozone sh key list /lab/raw
```

Saída esperada:
```
202604_NovoBolsaFamilia.csv
```

---

### 17.6 Disponibilizando o JAR do Ozone para o Spark

O Apache Spark não inclui suporte ao Ozone por padrão. Para usar o esquema `o3fs://`, o JAR precisa estar no **classpath do sistema** do Spark — ou seja, no diretório `/opt/spark/jars/` de todos os nós do cluster.

> **Por que não usar `--jars`?** JARs passados via `--jars` são carregados por um classloader filho. A classe `ProtobufRpcEngine` (usada pelo cliente Ozone) já está carregada pelo classloader pai a partir do `hadoop-client-api-3.4.1.jar`. Pelo modelo de delegação do Java, o pai não enxerga classes do filho — por isso o JAR precisa estar no classloader do sistema.

Copie o JAR para o diretório `jars/` em todos os containers do cluster. Como `docker cp` não suporta cópia direta entre containers, use o host como intermediário:

```bash
# Passo 1: extrair do container Ozone para o host
docker cp ozone-om:/opt/hadoop/share/ozone/lib/ozone-filesystem-hadoop3-client-2.1.0.jar \
    /tmp/ozone-filesystem-hadoop3.jar

# Passo 2: distribuir para todos os containers Spark
for container in spark-master spark-worker-1 spark-worker-2; do
    docker cp /tmp/ozone-filesystem-hadoop3.jar \
        ${container}:/opt/spark/jars/ozone-filesystem-hadoop3.jar
done
```

Verifique:

```bash
docker compose exec spark-master ls -lh /opt/spark/jars/ozone-filesystem-hadoop3.jar
```

> **Nota:** Esta instalação é válida enquanto os containers estiverem em execução. Após `docker compose down` e `docker compose up -d`, repita este passo.

---

### 17.7 Criando o script example-job.py

Crie o job PySpark que vai processar a base do Bolsa Família:

```bash
cat <<'EOF' > apps/example-job.py
"""
Apache Spark Cluster Lab
Job: Bolsa Família - Total de Pagamentos por UF

Author: Prof. Barbosa
Contact: infobarbosa@gmail.com

Uso:
    docker compose exec spark-master spark-submit /apps/example-job.py

Pré-requisito (ver Seção 17.6 do tutorial):
    docker cp ozone-om:/opt/hadoop/share/ozone/lib/ozone-filesystem-hadoop3-client-2.1.0.jar \
        /tmp/ozone-filesystem-hadoop3.jar
    for container in spark-master spark-worker-1 spark-worker-2; do
        docker cp /tmp/ozone-filesystem-hadoop3.jar \
            ${container}:/opt/spark/jars/ozone-filesystem-hadoop3.jar
    done
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, sum as spark_sum
from pyspark.sql.types import DoubleType

# ============================================
# Inicializar SparkSession com suporte ao Ozone
# ============================================
spark = SparkSession.builder \
    .appName("bolsa-familia-por-uf") \
    .config("spark.hadoop.fs.o3fs.impl",
            "org.apache.hadoop.fs.ozone.OzoneFileSystem") \
    .config("spark.hadoop.ozone.om.address", "ozone-om") \
    .config("spark.hadoop.ozone.replication", "1") \
    .getOrCreate()

print("=" * 60)
print(" SparkSession inicializada com sucesso!")
print(f" App Name : {spark.sparkContext.appName}")
print(f" Master   : {spark.sparkContext.master}")
print(f" Version  : {spark.version}")
print("=" * 60)

# ============================================
# Leitura do CSV a partir do Ozone (bucket raw)
# ============================================
INPUT_PATH = "o3fs://raw.lab/202604_NovoBolsaFamilia.csv"

df = spark.read.csv(
    INPUT_PATH,
    header=True,
    sep=";",
    encoding="ISO-8859-1"
)

print("\nSchema do dataset:")
df.printSchema()
print(f"Total de registros lidos: {df.count():,}")

# ============================================
# Transformação: converter separador decimal
# O campo VALOR PARCELA usa vírgula: "800,00" -> 800.00
# ============================================
df = df.withColumn(
    "VALOR",
    regexp_replace(col("VALOR PARCELA"), ",", ".").cast(DoubleType())
)

# ============================================
# Agregação: somar o total pago por UF
# ============================================
resultado = df.groupBy("UF") \
    .agg(spark_sum("VALOR").alias("TOTAL_PAGO")) \
    .orderBy("UF")

print("\nTotal de pagamentos por UF:")
resultado.show(30, truncate=False)

# ============================================
# Escrita do resultado no Ozone (bucket output)
# ============================================
OUTPUT_PATH = "o3fs://output.lab/bolsafamilia-por-uf"

resultado.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print(f"\nResultado gravado em: {OUTPUT_PATH}")
print("Job finalizado com sucesso!")
spark.stop()
EOF
```

---

### 17.8 Parâmetros de integração Spark ↔ Ozone

| Parâmetro | Valor | Descrição |
|-----------|-------|-----------|
| `spark.hadoop.fs.o3fs.impl` | `org.apache.hadoop.fs.ozone.OzoneFileSystem` | Registra a implementação do esquema `o3fs://` no Spark. Sem essa configuração, qualquer acesso a caminhos `o3fs://` falha com `No FileSystem for scheme`. |
| `spark.hadoop.ozone.om.address` | `ozone-om` | Hostname do Ozone Manager (OM). O Spark usa esse endereço para resolver os metadados de volumes, buckets e keys. |
| `spark.hadoop.ozone.replication` | `1` | Fator de replicação usado pelo cliente Ozone nos workers do Spark ao gravar dados. Os containers Spark não lêem o `ozone-site.xml` dos containers Ozone, então sem essa config o padrão `THREE` é usado — o que falha com apenas 1 DataNode. |
| `/opt/spark/jars/ozone-filesystem-hadoop3.jar` | *(instalação no sistema)* | O JAR deve estar no diretório `jars/` do Spark em todos os nós. Isso o coloca no classloader do sistema, permitindo que `ProtobufRpcEngine` (Hadoop 3.4.x) encontre as classes do protocolo RPC do Ozone. |
| `o3fs://bucket.volume/caminho` | *(URI de dados)* | Formato da URI do Ozone: `raw.lab` indica o bucket `raw` dentro do volume `lab`. O trecho após `/` é o nome da key (arquivo ou prefixo de diretório) dentro do bucket. |
| `sep=";"` | *(opção de leitura CSV)* | O arquivo usa ponto-e-vírgula como separador de campos, padrão em arquivos CSV brasileiros. |
| `encoding="ISO-8859-1"` | *(opção de leitura CSV)* | O arquivo usa codificação Latin-1, comum em exports de sistemas governamentais brasileiros. |
| `regexp_replace(..., ",", ".")` | *(transformação)* | Converte o separador decimal de vírgula para ponto antes do cast para `DoubleType`, necessário porque o Spark espera notação decimal com ponto. |

---

### 17.9 Submetendo o job

```bash
docker compose exec spark-master spark-submit /apps/example-job.py
```

Saída esperada (parcial):
```
============================================================
 SparkSession inicializada com sucesso!
 App Name : bolsa-familia-por-uf
 Master   : spark://spark-master:7077
 Version  : 4.0.3
============================================================

Total de pagamentos por UF:
+---+--------------------+
|UF |TOTAL_PAGO          |
+---+--------------------+
|AC |...                 |
|AL |...                 |
|AM |...                 |
...
+---+--------------------+

Resultado gravado em: o3fs://output.lab/bolsafamilia-por-uf
Job finalizado com sucesso!
```

> **Dica:** Enquanto o job estiver rodando, acesse `http://<IP_DO_HOST>:4040` para acompanhar o progresso na Application UI.

---

### 17.10 Verificando o resultado no Ozone

**Listar os arquivos de resultado gerados pelo job:**

```bash
docker compose exec ozone-om ozone sh key list --prefix bolsafamilia-por-uf lab/output

```

```bash
docker compose exec ozone-om ozone sh key list -a --prefix bolsafamilia-por-uf lab/output

```

**Baixar e visualizar um arquivo de resultado** (substitua `<NOME_DO_ARQUIVO>` por uma das keys listadas acima que comece com `part-`):

```bash
docker compose exec ozone-om ozone sh key get \
    /lab/output/bolsafamilia-por-uf/<NOME_DO_ARQUIVO> \
    /tmp/resultado.parquet
```

```bash
docker compose exec ozone-om parquet-tools show /tmp/resultado.parquet

```

Saída esperada:
```
UF,TOTAL_PAGO
AC,XXXXXXX.0
AL,XXXXXXX.0
AM,XXXXXXX.0
...
```

> **Nota:** O resultado completo também é exibido no console durante a execução do job, na saída do `resultado.show(30)`.

---

### 17.11 - Job de leitura dos resultados

```sh
echo <<'EOF' > /apps/read-results-job.py
"""
Apache Spark Cluster Lab
Job: Bolsa Família - Leitura dos Resultados por UF

Author: Prof. Barbosa
Contact: infobarbosa@gmail.com

Uso:
    docker compose exec spark-master spark-submit /apps/read-results.py

Pré-requisito:
    O script example-job.py deve ter sido executado previamente, pois este
    script lê o resultado gravado em o3fs://output.lab/bolsafamilia-por-uf.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number

# ============================================
# Inicializar SparkSession com suporte ao Ozone
# ============================================
spark = SparkSession.builder \
    .appName("bolsa-familia-read-results") \
    .config("spark.hadoop.fs.o3fs.impl",
            "org.apache.hadoop.fs.ozone.OzoneFileSystem") \
    .config("spark.hadoop.ozone.om.address", "ozone-om") \
    .config("spark.hadoop.ozone.replication", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" SparkSession inicializada com sucesso!")
print(f" App Name : {spark.sparkContext.appName}")
print(f" Master   : {spark.sparkContext.master}")
print(f" Version  : {spark.version}")
print("=" * 60)

# ============================================
# Leitura do resultado em Parquet a partir do Ozone
# ============================================
OUTPUT_PATH = "o3fs://output.lab/bolsafamilia-por-uf"

print(f"\nLendo resultado de: {OUTPUT_PATH}")

df = spark.read.parquet(OUTPUT_PATH)

# ============================================
# Exibição no terminal
# ============================================
print("\nSchema do resultado:")
df.printSchema()

total_ufs = df.count()
print(f"Total de UFs no resultado: {total_ufs}")

print("\n" + "=" * 60)
print("  TOTAL DE PAGAMENTOS DO BOLSA FAMÍLIA POR UF")
print("=" * 60)

df_formatado = df \
    .orderBy("UF") \
    .withColumn("TOTAL_PAGO_BRL", format_number("TOTAL_PAGO", 2))

df_formatado.select("UF", "TOTAL_PAGO_BRL").show(total_ufs, truncate=False)

# Total geral
total_geral = df.agg({"TOTAL_PAGO": "sum"}).collect()[0][0]
print(f"TOTAL GERAL (todas as UFs): R$ {total_geral:,.2f}")
print("=" * 60)

print("\nLeitura concluída com sucesso!")
spark.stop()
EOF

```

```sh
docker compose exec spark-master spark-submit /apps/read-results-job.py

```

## 18. Parando o Cluster

Para parar todos os containers (sem perder dados):

```bash
docker compose down
```

Para reiniciar o cluster posteriormente:

```bash
docker compose up -d
```

---

## 19. Limpeza Completa

Se você quiser remover **tudo** (containers, volumes, imagens):

```bash
# Parar e remover containers + volumes
docker compose down -v
```

```bash
# Remover a imagem do Spark
docker rmi spark-lab:4.0.3
```

```bash
# Remover a imagem do Ozone
docker rmi apache/ozone:2.1.0
```

---

## Parabéns! 🎉

Você construiu com sucesso um cluster Apache Spark com armazenamento Apache Ozone, tudo rodando em Docker!

**O que você aprendeu:**
- Construir uma imagem Docker do Spark **do zero** com Dockerfile
- Criar um script de entrypoint inteligente (master vs worker)
- Configurar o Spark em modo standalone
- Montar um cluster com Docker Compose usando **IPs fixos** e **health checks**
- Configurar o Apache Ozone com setup mínimo
- Submeter jobs PySpark para o cluster
