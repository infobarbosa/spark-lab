"""
Apache Spark Cluster Lab
Template para exercícios futuros

Author: Prof. Barbosa
Contact: infobarbosa@gmail.com

Uso:
    docker compose exec spark-master spark-submit /apps/example-job.py
"""

from pyspark.sql import SparkSession

# ============================================
# Inicializar SparkSession
# ============================================
spark = SparkSession.builder \
    .appName("example-job") \
    .getOrCreate()

print("=" * 50)
print(" SparkSession inicializada com sucesso!")
print(f" App Name : {spark.sparkContext.appName}")
print(f" Master   : {spark.sparkContext.master}")
print(f" Version  : {spark.version}")
print("=" * 50)

# ============================================
# TODO: Implemente seu job aqui
# ============================================

# Exemplo de leitura de CSV:
# df = spark.read.csv("/data/seu_arquivo.csv", header=True, sep=";")
# df.show(10)
# df.printSchema()
# print(f"Total de registros: {df.count()}")

# ============================================

print("Job finalizado com sucesso!")
spark.stop()
