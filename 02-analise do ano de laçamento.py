# Databricks notebook source
import pyspark.pandas  as  pd

# COMMAND ----------

df_data = pd.read_parquet("dbfs:/FileStore/siver/dados.parquet/")
df_data.head(5)

# COMMAND ----------

df_data.describe()

# COMMAND ----------

df_data.year.value_counts().sort_index().plot.bar()

# COMMAND ----------

df_data['decade'] = df_data.year.apply(lambda year: f'{(year//10)*10}s')


# COMMAND ----------

df_data.head(5)

# COMMAND ----------

df_data2 = df_data[['decade']]
df_data2['qtd'] = 1

# COMMAND ----------

df_data2 = df_data2.groupby('decade').sum()
df_data2

# COMMAND ----------

df_data2.sort_index().plot.bar()

# COMMAND ----------

year_file = "dbfs:/FileStore/tables/arquivos_curso/data_by_year.csv"
df_year = spark.read.csv(year_file,inferSchema=True, header=True)
df_year = df_year.pandas_api()
df_year.head(5)

# COMMAND ----------

len(df_year.year.unique())

# COMMAND ----------

df_year.plot.line(x='year', y='duration_ms')

# COMMAND ----------

df_year.plot.line(x='year', y=['acousticness', 'danceability',  'energy','instrumentalness', 'liveness', 'speechiness', 'valence'])

# COMMAND ----------

df_year['decade'] = df_year.year.apply(lambda year: f'{(year//10)*10}')
df_year.head(5)

# COMMAND ----------

df_year.plot.line(x='decade', y=['acousticness', 'danceability',  'energy','instrumentalness', 'liveness', 'speechiness', 'valence'])

# COMMAND ----------


