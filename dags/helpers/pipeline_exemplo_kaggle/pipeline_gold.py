from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, to_date, sum as f_sum, count as f_count,
    avg as f_avg, desc, concat, lit, lpad, round as f_round,
    to_timestamp, current_timestamp, countDistinct
)
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

from utils.datalake_manipulation import write_in_destination_v2, write_to_postgresql
from utils.spark_config import create_spark_session_local
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Configuração do banco de dados Gold
# ---------------------------------------------------------------------------
db_url      = Variable.get('url_postgres_gold')
db_user     = Variable.get('user_postgres_gold')
db_password = Variable.get('password_postgres_gold')
db_host     = Variable.get('host_postgres_gold')
db_port     = '5432'
db_name     = 'gold'


# ---------------------------------------------------------------------------
# Leitura e agregação das tabelas Silver
# ---------------------------------------------------------------------------

def read_silver_tables(spark: SparkSession) -> dict:
    """
    Lê todas as tabelas Silver necessárias e retorna um dicionário de DataFrames.
    Centralizar a leitura facilita testes e reuso.
    """
    base = './datalake/silver/Kaggle'

    return {
        'customers':            DeltaTable.forPath(spark, f'{base}/olist_customers_dataset/').toDF(),
        'orders':               DeltaTable.forPath(spark, f'{base}/olist_orders_dataset/').toDF(),
        'order_items':          DeltaTable.forPath(spark, f'{base}/olist_order_items_dataset/').toDF(),
        'products':             DeltaTable.forPath(spark, f'{base}/olist_products_dataset/').toDF(),
        'sellers':              DeltaTable.forPath(spark, f'{base}/olist_sellers_dataset/').toDF(),
        'category_translation': DeltaTable.forPath(spark, f'{base}/product_category_name_translation/').toDF(),
        'order_payments':       DeltaTable.forPath(spark, f'{base}/olist_order_payments_dataset/').toDF(),
    }


def build_gold_base(spark: SparkSession) -> 'DataFrame':
    """
    Constrói o DataFrame base da camada Gold resolvendo os dois problemas
    que causavam duplicação de linhas e valores inflados nas métricas:

    Problema 1 — order_payments tem múltiplas linhas por order_id
    (parcelas e meios de pagamento diferentes). Solução: agregar antes do join,
    somando payment_value e contando parcelas por pedido.

    Problema 2 — order_items tem múltiplas linhas por order_id
    (um registro por produto comprado). Solução: agregar antes do join,
    somando price + freight e contando itens por pedido.
    Depois disso o join com products/sellers é feito no nível de item,
    não no nível de pedido, preservando a granularidade correta.

    O DataFrame resultante tem uma linha por order_item (granularidade de item),
    com os totais de pagamento do pedido repetidos (correto para métricas de pedido)
    e sem explosão cartesiana.
    """
    tables = read_silver_tables(spark)

    # -- 1. Pré-agregar pagamentos: 1 linha por order_id --------------------
    payments_agg = (
        tables['order_payments']
        .groupBy('order_id')
        .agg(
            f_sum('payment_value').alias('payment_value'),
            f_count('payment_sequential').alias('payment_installments_count'),
            f.first('payment_type').alias('main_payment_type'),   # tipo mais frequente via first após ordenação
        )
    )

    # -- 2. Enriquecer order_items com produto e vendedor -------------------
    items_enriched = (
        tables['order_items']
        .join(tables['products'],  on='product_id', how='left')
        .join(tables['sellers'],   on='seller_id',  how='left')
        .join(tables['category_translation'], on='product_category_name', how='left')
    )

    # -- 3. Enriquecer orders com cliente e coluna de data ------------------
    orders_enriched = (
        tables['orders']
        .join(tables['customers'], on='customer_id', how='left')
        # Extrair ano e mês da data de compra para facilitar agregações temporais
        .withColumn(
            'order_purchase_timestamp',
            to_timestamp(col('order_purchase_timestamp'), 'yyyy-MM-dd HH:mm:ss')
        )
        .withColumn('purchase_year',  year(col('order_purchase_timestamp')))
        .withColumn('purchase_month', month(col('order_purchase_timestamp')))
        # Filtrar apenas pedidos entregues para métricas de receita
        .filter(col('order_status') == 'delivered')
    )

    # -- 4. Montar o DataFrame Gold -----------------------------------------
    df_gold = (
        orders_enriched
        .join(items_enriched,  on='order_id', how='left')
        .join(payments_agg,    on='order_id', how='left')
    )

    return df_gold


# ---------------------------------------------------------------------------
# Funções de agregação (camada Gold)
# ---------------------------------------------------------------------------

def calculate_monthly_sales(spark: SparkSession, df_gold: 'DataFrame') -> None:
    """
    Receita mensal total.
    Agrega payment_value no nível de pedido (distinct) para evitar
    contar o mesmo pagamento várias vezes quando há múltiplos itens.
    """
    path_to_write = './datalake/gold/sales/monthly_sales/'

    # Desduplicar no nível de pedido antes de somar pagamentos
    orders_dedup = df_gold.select(
        'order_id', 'purchase_year', 'purchase_month', 'payment_value'
    ).dropDuplicates(['order_id'])

    monthly_sales = (
        orders_dedup
        .groupBy('purchase_year', 'purchase_month')
        .agg(
            f_round(f_sum('payment_value'), 2).alias('total_sales'),
            f_count('order_id').alias('total_orders'),
            f_round(f_avg('payment_value'), 2).alias('avg_order_value'),
        )
        .withColumn(
            'month_year',
            concat(
                col('purchase_year').cast(StringType()),
                lit('-'),
                lpad(col('purchase_month').cast(StringType()), 2, '0')
            )
        )
        .select('month_year', 'purchase_year', 'purchase_month',
                'total_sales', 'total_orders', 'avg_order_value')
        .orderBy('purchase_year', 'purchase_month')
    )

    write_in_destination_v2(
        spark=spark,
        df_spark=monthly_sales,
        destination_path=path_to_write,
        primary_key=None,
        write_mode='overwrite'
    )

    write_to_postgresql(
        spark_df=monthly_sales,
        table_name='monthly_sales_gold',
        db_url=db_url, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port, db_name=db_name
    )


def calculate_top_product_categories(spark: SparkSession, df_gold: 'DataFrame', limit: int = 10) -> None:
    """
    Top N categorias de produtos por receita.
    Usa price (nível de item) em vez de payment_value (nível de pedido)
    para refletir a contribuição real de cada categoria.
    """
    path_to_write = './datalake/gold/product/top_product_categories/'

    top_categories = (
        df_gold
        .filter(col('product_category_name_english').isNotNull())
        .groupBy('product_category_name_english')
        .agg(
            f_round(f_sum('price'), 2).alias('total_revenue'),
            f_count('order_item_id').alias('total_items_sold'),
            countDistinct('order_id').alias('total_orders'),
            f_round(f_avg('price'), 2).alias('avg_item_price'),
        )
        .orderBy(desc('total_revenue'))
        .limit(limit)
    )

    write_in_destination_v2(
        spark=spark,
        df_spark=top_categories,
        destination_path=path_to_write,
        primary_key=None,
        write_mode='overwrite'
    )

    write_to_postgresql(
        spark_df=top_categories,
        table_name='top_product_categories',
        db_url=db_url, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port, db_name=db_name
    )


def calculate_sales_by_customer_state(spark: SparkSession, df_gold: 'DataFrame') -> None:
    """
    Receita e volume por estado do cliente.
    Desduplicado no nível de pedido para não inflar payment_value.
    """
    path_to_write = './datalake/gold/sales/sales_by_customer_state/'

    orders_dedup = df_gold.select(
        'order_id', 'customer_state', 'payment_value'
    ).dropDuplicates(['order_id'])

    sales_by_state = (
        orders_dedup
        .filter(col('customer_state').isNotNull())
        .groupBy('customer_state')
        .agg(
            f_round(f_sum('payment_value'), 2).alias('total_sales'),
            f_count('order_id').alias('total_orders'),
            f_round(f_avg('payment_value'), 2).alias('avg_order_value'),
        )
        .orderBy(desc('total_sales'))
    )

    write_in_destination_v2(
        spark=spark,
        df_spark=sales_by_state,
        destination_path=path_to_write,
        primary_key=None,
        write_mode='overwrite'
    )

    write_to_postgresql(
        spark_df=sales_by_state,
        table_name='sales_by_customer_state',
        db_url=db_url, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port, db_name=db_name
    )


def calculate_seller_performance(spark: SparkSession, df_gold: 'DataFrame') -> None:
    """
    [NOVO] Performance por vendedor: receita, volume de itens e ticket médio.
    Métrica no nível de item (price), que é a granularidade correta para vendedor.
    """
    path_to_write = './datalake/gold/sellers/seller_performance/'

    seller_perf = (
        df_gold
        .filter(col('seller_id').isNotNull())
        .groupBy('seller_id', 'seller_state', 'seller_city')
        .agg(
            f_round(f_sum('price'), 2).alias('total_revenue'),
            f_count('order_item_id').alias('total_items_sold'),
            countDistinct('order_id').alias('total_orders'),
            f_round(f_avg('price'), 2).alias('avg_item_price'),
        )
        .orderBy(desc('total_revenue'))
    )

    write_in_destination_v2(
        spark=spark,
        df_spark=seller_perf,
        destination_path=path_to_write,
        primary_key=None,
        write_mode='overwrite'
    )

    write_to_postgresql(
        spark_df=seller_perf,
        table_name='seller_performance',
        db_url=db_url, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port, db_name=db_name
    )


def calculate_delivery_metrics(spark: SparkSession, df_gold: 'DataFrame') -> None:
    """
    [NOVO] Métricas de entrega por estado: prazo médio real vs estimado.
    Útil para análise operacional e de satisfação do cliente.
    """
    path_to_write = './datalake/gold/logistics/delivery_metrics/'

    delivery = (
        df_gold
        .select('order_id', 'customer_state',
                'order_purchase_timestamp',
                'order_delivered_customer_date',
                'order_estimated_delivery_date')
        .dropDuplicates(['order_id'])
        .withColumn('delivered_date',  to_timestamp('order_delivered_customer_date',  'yyyy-MM-dd HH:mm:ss'))
        .withColumn('estimated_date',  to_timestamp('order_estimated_delivery_date',   'yyyy-MM-dd HH:mm:ss'))
        .withColumn('purchase_ts',     to_timestamp('order_purchase_timestamp',        'yyyy-MM-dd HH:mm:ss'))
        .withColumn(
            'delivery_days_actual',
            (col('delivered_date').cast('long') - col('purchase_ts').cast('long')) / 86400
        )
        .withColumn(
            'delivery_days_estimated',
            (col('estimated_date').cast('long') - col('purchase_ts').cast('long')) / 86400
        )
        .withColumn(
            'delivery_delay_days',
            col('delivery_days_actual') - col('delivery_days_estimated')
        )
        .filter(col('delivery_days_actual').isNotNull())
        .groupBy('customer_state')
        .agg(
            f_round(f_avg('delivery_days_actual'),   1).alias('avg_delivery_days'),
            f_round(f_avg('delivery_days_estimated'),1).alias('avg_estimated_days'),
            f_round(f_avg('delivery_delay_days'),    1).alias('avg_delay_days'),
            f_count('order_id').alias('total_orders'),
        )
        .orderBy('avg_delivery_days')
    )

    write_in_destination_v2(
        spark=spark,
        df_spark=delivery,
        destination_path=path_to_write,
        primary_key=None,
        write_mode='overwrite'
    )

    write_to_postgresql(
        spark_df=delivery,
        table_name='delivery_metrics',
        db_url=db_url, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port, db_name=db_name
    )


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------

def pipeline_kaggle_gold() -> None:
    spark = create_spark_session_local()

    print('Construindo DataFrame Gold base...')
    df_gold = build_gold_base(spark)
    print(f'DataFrame Gold base criado — {df_gold.count()} linhas')

    print('Calculando monthly_sales...')
    calculate_monthly_sales(spark, df_gold)
    print('monthly_sales — OK')

    print('Calculando top_product_categories...')
    calculate_top_product_categories(spark, df_gold, limit=10)
    print('top_product_categories — OK')

    print('Calculando sales_by_customer_state...')
    calculate_sales_by_customer_state(spark, df_gold)
    print('sales_by_customer_state — OK')

    print('Calculando seller_performance...')
    calculate_seller_performance(spark, df_gold)
    print('seller_performance — OK')

    print('Calculando delivery_metrics...')
    calculate_delivery_metrics(spark, df_gold)
    print('delivery_metrics — OK')

    print('Pipeline Gold concluída com sucesso.')