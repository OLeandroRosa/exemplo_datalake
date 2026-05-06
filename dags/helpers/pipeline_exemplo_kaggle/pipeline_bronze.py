from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date
import pyspark.sql.functions as f
from datetime import datetime

from utils.datalake_manipulation import write_in_destination_v2
from utils.spark_config import create_spark_session_local
from helpers.pipeline_exemplo_kaggle.objects_bronze import (
    customer_schema,
    geolocation_schema,
    order_item_schema,
    order_payment_schema,
    order_review_schema,
    order_schema,
    product_schema,
    category_translation_schema,
    seller_schema
)

    
def read_customer_kaggle(spark, path_to_read, schema, path_to_write):
    
    df_customers = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )
    
    df_customers = df_customers.select([
        f.col('customer_id').alias('customer_id'),
        f.col('customer_unique_id').alias('customer_unique_id'),
        f.col('customer_zip_code_prefix').alias('customer_zip_code_prefix'),
        f.col('customer_city').alias('customer_city'),
        f.col('customer_state').alias('customer_state'),

    ])
    
    primary_key = ['customer_id','customer_unique_id']


    write_in_destination_v2(spark=spark,
                        df_spark=df_customers,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')

def read_geolocation_kaggle(spark, path_to_read, schema, path_to_write):
    df_geolocation = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_geolocation = df_geolocation.select([
        f.col('geolocation_zip_code_prefix').alias('geolocation_zip_code_prefix'),
        f.col('geolocation_lat').alias('geolocation_lat'),
        f.col('geolocation_lng').alias('geolocation_lng'),
        f.col('geolocation_city').alias('geolocation_city'),
        f.col('geolocation_state').alias('geolocation_state')
    ])

    primary_key = ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

    write_in_destination_v2(spark=spark,
                        df_spark=df_geolocation,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append') 

def read_order_items_kaggle(spark, path_to_read, schema, path_to_write):
    df_order_items = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_order_items = df_order_items.select([
        f.col('order_id').alias('order_id'),
        f.col('order_item_id').alias('order_item_id'),
        f.col('product_id').alias('product_id'),
        f.col('seller_id').alias('seller_id'),
        f.col('shipping_limit_date').alias('shipping_limit_date'),
        f.col('price').alias('price'),
        f.col('freight_value').alias('freight_value')
    ])

    primary_key = ['order_id', 'order_item_id', 'product_id']

    write_in_destination_v2(spark=spark,
                        df_spark=df_order_items,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append') 

def read_order_payments_kaggle(spark, path_to_read, schema, path_to_write):
    df_order_payments = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_order_payments = df_order_payments.select([
        f.col('order_id').alias('order_id'),
        f.col('payment_sequential').alias('payment_sequential'),
        f.col('payment_type').alias('payment_type'),
        f.col('payment_installments').alias('payment_installments'),
        f.col('payment_value').alias('payment_value')
    ])

    primary_key = ['order_id', 'payment_sequential']

    write_in_destination_v2(spark=spark,
                        df_spark=df_order_payments,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')

def read_order_reviews_kaggle(spark, path_to_read, schema, path_to_write):
    df_order_reviews = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_order_reviews = df_order_reviews.select([
        f.col('review_id').alias('review_id'),
        f.col('order_id').alias('order_id'),
        f.col('review_score').alias('review_score'),
        f.col('review_comment_title').alias('review_comment_title'),
        f.col('review_comment_message').alias('review_comment_message'),
        f.col('review_creation_date').alias('review_creation_date'),
        f.col('review_answer_timestamp').alias('review_answer_timestamp')
    ])

    primary_key = ['review_id', 'order_id']

    write_in_destination_v2(spark=spark,
                        df_spark=df_order_reviews,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')

def read_orders_kaggle(spark, path_to_read, schema, path_to_write):
    df_orders = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_orders = df_orders.select([
        f.col('order_id').alias('order_id'),
        f.col('customer_id').alias('customer_id'),
        f.col('order_status').alias('order_status'),
        f.col('order_purchase_timestamp').alias('order_purchase_timestamp'),
        f.col('order_approved_at').alias('order_approved_at'),
        f.col('order_delivered_carrier_date').alias('order_delivered_carrier_date'),
        f.col('order_delivered_customer_date').alias('order_delivered_customer_date'),
        f.col('order_estimated_delivery_date').alias('order_estimated_delivery_date')
    ])

    primary_key = ['order_id']

    write_in_destination_v2(spark=spark,
                        df_spark=df_orders,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')

def read_products_kaggle(spark, path_to_read, schema, path_to_write):
    df_products = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_products = df_products.select([
        f.col('product_id').alias('product_id'),
        f.col('product_category_name').alias('product_category_name'),
        f.col('product_name_lenght').alias('product_name_lenght'),
        f.col('product_description_lenght').alias('product_description_lenght'),
        f.col('product_photos_qty').alias('product_photos_qty'),
        f.col('product_weight_g').alias('product_weight_g'),
        f.col('product_length_cm').alias('product_length_cm'),
        f.col('product_height_cm').alias('product_height_cm'),
        f.col('product_width_cm').alias('product_width_cm')
    ])

    primary_key = ['product_id']

    write_in_destination_v2(spark=spark,
                        df_spark=df_products,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')

def read_sellers_kaggle(spark, path_to_read, schema, path_to_write):
    df_sellers = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_sellers = df_sellers.select([
        f.col('seller_id').alias('seller_id'),
        f.col('seller_zip_code_prefix').alias('seller_zip_code_prefix'),
        f.col('seller_city').alias('seller_city'),
        f.col('seller_state').alias('seller_state')
    ])

    primary_key = ['seller_id']

    write_in_destination_v2(spark=spark,
                        df_spark=df_sellers,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')
  
def read_category_translation_kaggle(spark, path_to_read, schema, path_to_write):
    df_category_translation = spark.read.csv(
        path=path_to_read,
        schema=schema,
        header=True,
        sep=','
    )

    df_category_translation = df_category_translation.select([
        f.col('product_category_name').alias('product_category_name'),
        f.col('product_category_name_english').alias('product_category_name_english')
    ])

    primary_key = ['product_category_name']

    write_in_destination_v2(spark=spark,
                        df_spark=df_category_translation,
                        destination_path=path_to_write,
                        primary_key=primary_key,
                        write_mode='append')


def pipeline_kaggle_bronze(source_path, file_name):
    
    now = datetime.now().strftime("%Y-%m-%d").split('-') 
    spark = create_spark_session_local()
    file_name = file_name.replace('.csv', '')

    destination_path = f'./datalake/bronze/Kaggle/{file_name}/year={now[0]}/month={now[1]}/day={now[2]}'
    print('Lendo arquiva no caminho: ', source_path)
    
    if file_name == 'olist_customers_dataset':
        read_customer_kaggle(spark=spark,
                             path_to_read=source_path,
                             schema=customer_schema,
                             path_to_write=destination_path)
        
    elif file_name == 'olist_orders_dataset':
        read_orders_kaggle(spark=spark,
                           path_to_read=source_path,
                           schema=order_schema,
                           path_to_write=destination_path)
        
    elif file_name == 'olist_sellers_dataset':
        read_sellers_kaggle(spark=spark,
                            path_to_read=source_path,
                            schema=seller_schema,
                            path_to_write=destination_path)
        
    elif file_name == 'olist_geolocation_dataset':
        read_geolocation_kaggle(spark=spark,
                                path_to_read=source_path,
                                schema=geolocation_schema,
                                path_to_write=destination_path)
    elif file_name == 'olist_order_items_dataset':
        read_order_items_kaggle(spark=spark,
                                path_to_read=source_path,
                                schema=order_item_schema,
                                path_to_write=destination_path)
    elif file_name == 'olist_order_payments_dataset':
        read_order_payments_kaggle(spark=spark,
                                   path_to_read=source_path,
                                   schema=order_payment_schema,
                                   path_to_write=destination_path)
    elif file_name == 'olist_order_reviews_dataset':
        read_order_reviews_kaggle(spark=spark,
                                  path_to_read=source_path,
                                  schema=order_review_schema,
                                  path_to_write=destination_path)
        
    elif file_name == 'olist_products_dataset':
        read_products_kaggle(spark=spark,
                             path_to_read=source_path,
                             schema=product_schema,
                             path_to_write=destination_path)
        
    elif file_name == 'product_category_name_translation':
        read_category_translation_kaggle(spark=spark,
                                         path_to_read=source_path,
                                         schema=category_translation_schema,
                                         path_to_write=destination_path)
        
    else:
        print(f"File name '{file_name}' not recognized for pipeline.")

    print(f"Pipeline para {file_name} executado com sucesso!")