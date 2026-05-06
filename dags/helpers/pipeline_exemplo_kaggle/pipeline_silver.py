from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date
import pyspark.sql.functions as f
from datetime import datetime
from delta.tables import DeltaTable

from utils.datalake_manipulation import write_in_destination_v2
from utils.spark_config import create_spark_session_local
from helpers.pipeline_exemplo_kaggle.objects_silver import standardize_name_udf
from pyspark.sql.functions import udf, initcap, trim, col, current_timestamp, to_timestamp

    
def transform_customer_kaggle(path_to_read, path_to_write):
    
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_customers = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['customer_id', 'customer_unique_id']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # Criação do campos de timestamp com a data da nova carga na camada Silver
        
        df_customers_transformed = df_customers.select([
            f.col('customer_id'),
            f.col('customer_unique_id'),
            f.col('customer_zip_code_prefix'),
            f.col('customer_city'),
            f.col('customer_state')
        

        ]).na.fill({'customer_city': 'Unknown', 'customer_state': 'Unknown'}) \
        .dropDuplicates(primary_key) \

        write_in_destination_v2(
            spark=spark,
            df_spark=df_customers_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de customer_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de customer_kaggle: {e}')
        raise e
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_orders_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_orders = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['order_id']
        
        # tratamento de valores ausentes
        # padronização dos nomes dos estados, cidades e status
        # Remover duplicados, com base na chave primária
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_orders_transformed = df_orders.select([
            f.col('order_id'),
            f.col('customer_id'),
            f.col('order_status'),
            f.col('order_purchase_timestamp'),
            f.col('order_approved_at'),
            f.col('order_delivered_carrier_date'),
            f.col('order_delivered_customer_date'),
            f.col('order_estimated_delivery_date')
            

        ]) \
        .withColumn('order_status', f.lower(f.col('order_status'))) \
        .withColumn('order_purchase_timestamp', to_timestamp(f.col('order_purchase_timestamp'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('order_approved_at', to_timestamp(f.col('order_approved_at'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('order_delivered_carrier_date', to_timestamp(f.col('order_delivered_carrier_date'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('order_delivered_customer_date', to_timestamp(f.col('order_delivered_customer_date'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('order_estimated_delivery_date', to_timestamp(f.col('order_estimated_delivery_date'), 'yyyy-MM-dd HH:mm:ss')) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_orders_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de orders_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de orders_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')

def transform_sellers_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_sellers = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['seller_id']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_sellers_transformed = df_sellers.select([
            f.col('seller_id'),
            f.col('seller_zip_code_prefix'),
            f.col('seller_city'),
            f.col('seller_state')
        ]).na.fill({'seller_city': 'Unknown', 'seller_state': 'Unknown'}) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_sellers_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de sellers_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de sellers_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_geolocation_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_geolocation = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_geolocation_transformed = df_geolocation.select([
            f.col('geolocation_zip_code_prefix'),
            f.col('geolocation_lat'),
            f.col('geolocation_lng'),
            f.col('geolocation_city'),
            f.col('geolocation_state')
        ]).na.fill({'geolocation_city': 'Unknown', 'geolocation_state': 'Unknown'}) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_geolocation_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de geolocation_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de geolocation_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_order_items_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_order_items = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['order_id', 'order_item_id', 'product_id']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_order_items_transformed = df_order_items.select([
            f.col('order_id'),
            f.col('order_item_id'),
            f.col('product_id'),
            f.col('seller_id'),
            f.col('shipping_limit_date'),
            f.col('price'),
            f.col('freight_value')
        ]).na.fill({'price': 0.0, 'freight_value': 0.0}) \
        .withColumn('shipping_limit_date', to_timestamp(f.col('shipping_limit_date'), 'yyyy-MM-dd HH:mm:ss')) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_order_items_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de order_items_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de order_items_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_order_payments_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_order_payments = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['order_id', 'payment_sequential']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # padronização do campo payment_type
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_order_payments_transformed = df_order_payments.select([
            f.col('order_id'),
            f.col('payment_sequential'),
            f.col('payment_type'),
            f.col('payment_installments'),
            f.col('payment_value')
        ]).na.fill({'payment_value': 0.0, 'payment_type': 'unknown'}) \
        .withColumn('payment_type', f.lower(f.col('payment_type'))) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_order_payments_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de order_payments_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de order_payments_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_order_reviews_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_order_reviews = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['review_id', 'order_id']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # Formatação dos campos de datas
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_order_reviews_transformed = df_order_reviews.select([
            f.col('review_id'),
            f.col('order_id'),
            f.col('review_score'),
            f.col('review_comment_title'),
            f.col('review_comment_message'),
            f.col('review_creation_date'),
            f.col('review_answer_timestamp')
        ]).na.fill({'review_score': 0, 'review_comment_title': '', 'review_comment_message': ''}) \
        .withColumn('review_creation_date', to_timestamp(f.col('review_creation_date'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('review_answer_timestamp', to_timestamp(f.col('review_answer_timestamp'), 'yyyy-MM-dd HH:mm:ss')) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_order_reviews_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de order_reviews_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de order_reviews_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_products_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_products = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['product_id']
        
        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # padronização do campo product_category_name
        # Criação do campos de timestamp com a data da nova carga na camada Silver

        df_products_transformed = df_products.select([
            f.col('product_id'),
            f.col('product_category_name'),
            f.col('product_name_lenght'),
            f.col('product_description_lenght'),
            f.col('product_photos_qty'),
            f.col('product_weight_g'),
            f.col('product_length_cm'),
            f.col('product_height_cm'),
            f.col('product_width_cm')
        ]).na.fill({'product_category_name': 'unknown',
                    'product_name_lenght': 0, 'product_description_lenght': 0,
                    'product_photos_qty': 0, 'product_weight_g': 0.0,
                    'product_length_cm': 0.0, 'product_height_cm': 0.0,
                    'product_width_cm': 0.0}) \
        .withColumn('product_category_name', f.lower(f.col('product_category_name'))) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_products_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de products_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de products_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def transform_category_translation_kaggle(path_to_read, path_to_write):
    print('Inicializando Spark...')
    spark = create_spark_session_local()
    print('Spark Inicializado !!!')

    try:
        df_category_translation = DeltaTable.forPath(spark, path_to_read).toDF()

        primary_key = ['product_category_name']


        # tratamento de valores ausentes
        # Remover duplicados, com base na chave primária
        # padronização do campo product_category_name, product_category_name_english
        # Criação do campos de timestamp com a data da nova carga na cama
        df_category_translation_transformed = df_category_translation.select([
            f.col('product_category_name'),
            f.col('product_category_name_english')
        ]).na.fill({'product_category_name_english': 'unknown'}) \
        .withColumn('product_category_name', f.lower(f.col('product_category_name'))) \
        .withColumn('product_category_name_english', f.lower(f.col('product_category_name_english'))) \
        .dropDuplicates(primary_key) \
        .withColumn('dt_update', current_timestamp())

        write_in_destination_v2(
            spark=spark,
            df_spark=df_category_translation_transformed,
            destination_path=path_to_write,
            primary_key=primary_key,
            write_mode='merge'
        )
        print('Transformação e escrita de category_translation_kaggle concluídas com sucesso!')
    except Exception as e:
        print(f'Erro na transformação de category_translation_kaggle: {e}')
    finally:
        spark.stop()
        print('Spark Finalizado !!!')


def pipeline_kaggle_silver(file_name):
    
    now = datetime.now().strftime("%Y-%m-%d").split('-') 
    spark = create_spark_session_local()
    file_base_name = file_name.replace('.csv', '')
    
    
    path_read_bronze = f'./datalake/bronze/Kaggle/{file_base_name}/year={now[0]}/month={now[1]}/day={now[2]}'
    path_write_silver = f'./datalake/silver/Kaggle/{file_base_name}/'

    print(f"\n--- Iniciando transformação para {file_base_name} ---")
    if file_base_name == 'olist_customers_dataset':
        transform_customer_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_orders_dataset':
        transform_orders_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_sellers_dataset':
        transform_sellers_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_geolocation_dataset':
        transform_geolocation_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_order_items_dataset':
        transform_order_items_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_order_payments_dataset':
        transform_order_payments_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_order_reviews_dataset':
        transform_order_reviews_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'olist_products_dataset':
        transform_products_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
        
        
    elif file_base_name == 'product_category_name_translation':
        transform_category_translation_kaggle(path_to_read=path_read_bronze, path_to_write=path_write_silver)
    else:
        print(f"Nome do arquivo '{file_base_name}' não reconhecido para pipeline de Silver.")

    print(f"--- Transformação para {file_base_name} concluída ---")