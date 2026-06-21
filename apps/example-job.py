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
    .option("header", "true") \
    .csv(OUTPUT_PATH)

print(f"\nResultado gravado em: {OUTPUT_PATH}")
print("Job finalizado com sucesso!")
spark.stop()
