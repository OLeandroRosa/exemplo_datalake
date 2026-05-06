
# import boto3
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
import os

from delta.tables import *
from delta.tables import DeltaTable
from datetime import datetime

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, FloatType
from sqlalchemy import create_engine


from utils.spark_config import (create_spark_session,
                                create_spark_session_from_role,
                                create_boto_session_from_role
                                )



def read_local_files_write_in_raw(data_schema, local_path, raw_path):
  
  print('Inicializando Spark...')
  aws_key, aws_secret, _ = create_boto_session_from_role()
  spark = create_spark_session_from_role(aws_key, aws_secret)

  print('Spark Inicializado !!!')
  # spark = create_spark_session()

  df_spark = None

  try:
    # Realizando a leitura dos dados do diretório local
    print('\nLendo dados do repositório local...')
    df_spark = spark.read.schema(data_schema).json(path=local_path, multiLine=True)
    df_spark = df_spark.withColumn("timestamp_sensor", f.from_unixtime(f.col("timestamp_sensor"), "yyyy-MM-dd HH:mm:ss") )
    df_spark = df_spark.withColumn("month_year", f.date_format(f.col("timestamp_sensor"), "MM-yyyy"))

    print('Dados do repositório local lidos com sucesso')
  except Exception as e:
    print(e)
    return

  if df_spark != None:
      delta_table = None
      
      # Salvando os dados na Camada RAW
      try: 
        delta_table = DeltaTable.forPath(spark, raw_path)
        delta_table.write.format("delta")\
                          .partitionBy(["month_year"])\
                          .mode("append").save(raw_path)
      except Exception as e:
        print(e)
        df_spark.write.format("delta")\
                      .partitionBy(["month_year"])\
                      .mode("overwrite").save(raw_path)

  print('\nDados Gravados com sucesso!!!')


def write_in_destination(spark, df_spark, destination_path:str, primary_key:list, overwrite=False):

  pk_string = ''

  for pk in primary_key:
    if pk_string == '':
      pk_string = f'(target.{pk} = source.{pk})'
    else:
      pk_string += f'and (target.{pk} = source.{pk})'


  if df_spark != None:
      delta_table = None
      
      # Salvando os dados na Camada RAW
      try: 
        delta_table = DeltaTable.forPath(spark, destination_path)

        print('Possui DeltaTable no Destino')
        if overwrite:
          df_spark.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .save(destination_path)
        else:
  
          delta_table.alias('target')\
                     .merge(
                         df_spark.alias('source'),
                         pk_string)\
                     .whenMatchedUpdateAll()\
                     .whenNotMatchedInsertAll()\
                     .execute()
      except Exception as e:
        if not delta_table:
          print('Não possui DeltaTable no Destino')
          df_spark.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .save(destination_path)
        else:
          print(f"Erro inesperado: {str(e)}")


  print('Dados Gravados com sucesso!!!')
  
  
def write_in_destination_v2(spark, df_spark, destination_path:str, primary_key:list, write_mode:str='merge'):
  """
  Escreve um DataFrame PySpark em uma tabela Delta Lake.

  Args:
      spark: O objeto SparkSession (Instancia do Spark).
      df_spark: O DataFrame PySpark a ser escrito.
      destination_path: O caminho para a tabela Delta Lake.
      primary_key: Uma lista de nomes de colunas a serem usadas como chaves primárias para operações de merge.
      write_mode: O modo de escrita. Pode ser 'overwrite', 'append' ou 'merge'.
                  'overwrite': Sobrescreve toda a tabela se ela existir, cria caso contrário.
                  'append': Adiciona novos dados à tabela se ela existir, cria caso contrário.
                  'merge': Realiza uma operação de upsert usando primary_key se a tabela existir,
                           cria a tabela com modo 'overwrite' caso ela não exista (para inicializar o merge).
  """
  pk_string = ''
  if primary_key != None:
    for pk in primary_key:
      if pk_string == '':
        pk_string = f'(target.{pk} <=> source.{pk})'
      else:
        pk_string += f'and (target.{pk} <=> source.{pk})'


  if df_spark is not None:
      try:
        delta_table = DeltaTable.forPath(spark, destination_path)
        print(f'Possui DeltaTable no Destino. Aplicando modo: {write_mode}.')

        if write_mode == 'overwrite':
          df_spark.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .save(destination_path)
        elif write_mode == 'append':
          df_spark.write.format("delta")\
                        .mode("append")\
                        .save(destination_path)
        elif write_mode == 'merge':
          delta_table.alias('target')\
                     .merge(
                         df_spark.alias('source'),
                         pk_string)\
                     .whenMatchedUpdateAll()\
                     .whenNotMatchedInsertAll()\
                     .execute()
        else:
          print(f"Modo de escrita '{write_mode}' inválido. Use 'overwrite', 'append' ou 'merge'.")

      except Exception as e:
        # If DeltaTable.forPath fails, the table does not exist
        print(f'Não possui DeltaTable no Destino. Criando nova tabela com modo "overwrite" (para inicialização).')
        df_spark.write.format("delta")\
                      .mode("overwrite")\
                      .option("overwriteSchema", "true")\
                      .save(destination_path)


  print('Dados Gravados com sucesso!!!')
  
  
def write_to_postgresql(spark_df, table_name, db_url, db_user, db_password, db_host, db_port, db_name):
    """
    Escreve um PySpark DataFrame para uma tabela PostgreSQL.

    Args:
        spark_df (pyspark.sql.DataFrame): O DataFrame PySpark a ser escrito.
        table_name (str): O nome da tabela no PostgreSQL.
        db_url (str): O dialeto do banco (e.g., 'postgresql').
        db_user (str): O nome de usuário do banco de dados.
        db_password (str): A senha do banco de dados.
        db_host (str): O host do banco de dados.
        db_port (str): A porta do banco de dados.
        db_name (str): O nome do banco de dados.
    """
    print(f"Iniciando escrita do DataFrame '{table_name}' no PostgreSQL...")
    try:
        pandas_df = spark_df.toPandas()

        # String de conexão corrigida: usa db_name, não table_name
        db_connection_str = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(db_connection_str)

        pandas_df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"DataFrame '{table_name}' escrito com sucesso no PostgreSQL.")
    except Exception as e:
        print(f"Erro ao escrever o DataFrame '{table_name}' no PostgreSQL: {e}")
        raise  # Re-lança a exceção para o Airflow registrar a falha corretamente