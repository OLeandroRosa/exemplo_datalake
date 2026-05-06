import boto3
import boto3.session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date
import boto3

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/datasets/.env")

def create_boto_session_from_role():
    session = boto3.session.Session(
                        aws_access_key_id=os.getenv("AWS_KEY"),
                        aws_secret_access_key =os.getenv("AWS_SECRET"),
                        region_name = 'us-east-1'
                        )

    credentials = session.get_credentials()

    boto3.setup_default_session(
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_session_token=credentials.token,
        )
    
    return credentials.access_key, credentials.secret_key, credentials.token


def create_spark_session_from_role(aws_key, aws_secret):
    """This function creates the spark session
    from IAM role.
    Args:
        aws_key: AWS  temporary acess key
        aws_secret: temporary acess secret
        aws_token: session token
    Returns:
        A spark session with s3 integration.
    """
    spark_jars_packages = 'com.amazonaws:aws-java-sdk:1.12.246,org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.2.1'
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Airflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "3600000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.threads.max", "1000")
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
        # .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        # .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    return spark



def create_spark_session():
    
    """This function creates the spark session
    from IAM role.
    Args:
        aws_key: AWS  temporary acess key
        aws_secret: temporary acess secret
        aws_token: session token
    Returns:
        A spark session with s3 integration.
    """
    spark_jars_packages = 'com.amazonaws:aws-java-sdk:1.12.246,org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.2.1'
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Airflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "3600000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.threads.max", "1000")
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .getOrCreate()
    )

    return spark


def create_spark_session_local():

    """This function creates the spark session
    from IAM role.
    Args:
        aws_key: AWS  temporary acess key
        aws_secret: temporary acess secret
        aws_token: session token
    Returns:
        A spark session with s3 integration.
    """
    spark_jars_packages = 'com.amazonaws:aws-java-sdk:1.12.246,org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.2.1'
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Airflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "3600000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.threads.max", "1000")
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )

    return spark