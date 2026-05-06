
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# Define schema for olist_customers_dataset.csv
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# Define schema for olist_orders_dataset.csv
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True), # Read as string, cast later if needed
    StructField("order_approved_at", StringType(), True),        # Read as string, cast later if needed
    StructField("order_delivered_carrier_date", StringType(), True), # Read as string, cast later if needed
    StructField("order_delivered_customer_date", StringType(), True),# Read as string, cast later if needed
    StructField("order_estimated_delivery_date", StringType(), True) # Read as string, cast later if needed
])

# Define schema for olist_sellers_dataset.csv
seller_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

# Define schema for olist_geolocation_dataset.csv
geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True)
])

# Define schema for olist_order_items_dataset.csv
order_item_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

# Define schema for olist_order_payments_dataset.csv
order_payment_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

# Define schema for olist_order_reviews_dataset.csv
order_review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True)
])

# Define schema for olist_products_dataset.csv
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True)
])

# Define schema for product_category_name_translation.csv
category_translation_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True)
])