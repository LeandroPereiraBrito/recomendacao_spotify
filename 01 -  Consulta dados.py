# Databricks notebook source
import pandas as pd
import requests
from pyspark.sql import SparkSession
import pyspark.pandas as ps

# COMMAND ----------

# vrificar os diretórios existentes 
display(dbutils.fs.ls("dbfs:/FileStore/tables/arquivos_curso/")) 
#acessar o arquivo desejado 
path = "dbfs:/FileStore/tables/arquivos_curso/data.csv"
# Lêr o diretório 
df_data = spark.read.csv(path, inferSchema=True, header=True)

# COMMAND ----------

# Transforma o spark em pandas.api 
df_data = df_data.pandas_api()
# Checar os typos das colunas 
df_data.info() 

# COMMAND ----------

# Gerar a lista com os tipos da colunas conforme documentação  
colunas_float = ['acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'tempo', 'valence']
colunas_int = ['duration_ms', 'mode', 'key', 'explicit', 'popularity']
# Realizar a conversão  
df_data[colunas_float] = df_data[colunas_float].astype(float)
df_data[colunas_int] = df_data[colunas_int].astype(int)


# COMMAND ----------

#Apresentar
df_data.head()

# COMMAND ----------

# Ajustar o nome do artistis
df_data['artists'] = df_data.artists.str.replace("\[|\]|\'", "")

# COMMAND ----------

# Gerar pasta silver 
dbutils.fs.mkdirs("dbfs:/FileStore/siver/")
#Gravar 
df_data.to_parquet("dbfs:/FileStore/siver/dados.parquet")
