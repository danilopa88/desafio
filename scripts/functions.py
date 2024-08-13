from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    when, 
    lit, 
    split, 
    last, 
    regexp_replace, 
    to_date, 
    year
)
from pyspark.sql.window import Window

def create_spark_session(app_name):
    """
    Create and return a SparkSession with the given application name.

    Parameters:
    - app_name (str): The name of the Spark application.

    Returns:
    - SparkSession: A SparkSession object configured with the provided application name.
    """
    return (
        SparkSession.builder
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .appName(app_name)
        .getOrCreate()
    )

def read_data(spark, path, format="text", sep=None, header=None):
    """
    Read data from a specified path into a DataFrame.

    Parameters:
    - spark (SparkSession): The SparkSession used to read the data.
    - path (str): The path to the data source.
    - format (str, optional): The format of the data source (default is "text").
    - sep (str, optional): The delimiter used in the data (default is None, which means default delimiter for the format).
    - header (str, optional): Whether the data has a header row (default is None, which means use default behavior).

    Returns:
    - DataFrame: A DataFrame containing the loaded data.

    Raises:
    - Exception: If there is an error reading the data.
    """
    try:
        return (
            spark.read
            .format(format)
            .option("sep", sep if sep else ",")
            .option("header", header)
            .load(path)
        )
    except Exception as e:
        print(f"Error reading data from {path}: {e}")
        raise

def forward_fill(column):
    """
    Perform forward fill operation on a column to fill missing values using the last non-null value.

    Parameters:
    - column (Column): The column on which forward fill operation will be performed.

    Returns:
    - Column: A column with forward filled values.
    """
    window = Window.rowsBetween(float('-inf'), 0)
    return last(column, ignorenulls=True).over(window)

def process_netflix_data(df):
    """
    Process Netflix data by extracting and transforming columns.

    Parameters:
    - df (DataFrame): A DataFrame containing Netflix data with a column named 'value' that needs processing.

    Returns:
    - DataFrame: A DataFrame with transformed columns including 'movie_id', 'customer_id', 'ranking', and 'date'.
    """
    df = df.withColumn(
        "movie_id",
        when(col("value").endswith(":"), col("value")).otherwise(lit(None))
    )
    df = df.withColumn("movie_id", forward_fill(col("movie_id")))
    df = df.withColumn("movie_id", regexp_replace("movie_id", ":", ""))

    splitted = split(df.value, ',')
    df = df.withColumn("customer_id", splitted.getItem(0))
    df = df.withColumn("ranking", splitted.getItem(1))
    df = df.withColumn("date", splitted.getItem(2))

    df = (
        df.withColumn("movie_id", col("movie_id").cast("string"))
        .withColumn("customer_id", col("customer_id").cast("string"))
        .withColumn("ranking", col("ranking").cast("int"))
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    )

    return df.dropna().select("movie_id", "customer_id", "ranking", "date")

def process_amazon_data(amazon_df):
    """
    Process Amazon data by transforming columns.

    Parameters:
    - amazon_df (DataFrame): A DataFrame containing Amazon data with columns such as 'product_id', 'customer_id', 'star_rating', 'review_date', 'product_title', and 'marketplace'.

    Returns:
    - DataFrame: A DataFrame with transformed columns including 'movie_id', 'customer_id', 'ranking', 'date', 'title', 'year', and 'source'.
    """
    return (
        amazon_df.withColumn("movie_id", col("product_id").cast("string"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("ranking", col("star_rating").cast("int"))
        .withColumn("date", to_date(col("review_date"), "yyyy-MM-dd"))
        .withColumn("year", year(col("date")))
        .withColumn("title", col("product_title").cast("string"))
        .withColumn("source", lit('amazon'))
        .filter(col('marketplace')=='US')
        .select("customer_id", "movie_id", "ranking", "date", "title", "year", "source")
    )

def save_data(df, zone, table_name, format="parquet"):
    """
    Save a DataFrame to a specified S3 bucket location in the given format.

    Parameters:
    - df (DataFrame): The DataFrame to be saved.
    - zone (str): The zone (directory) within the S3 bucket where the data will be saved.
    - table_name (str): The name of the table (file) to be saved in the S3 bucket.
    - format (str, optional): The format in which to save the data (default is "parquet").

    Returns:
    - None

    Raises:
    - Exception: If there is an error saving the data.
    """
    try:
        df.write \
                .format(format) \
                .mode("overwrite") \
                .save(f"s3://datalake-desafio-1/{zone}/tables/{table_name}")
        print(f"Successfully saved {table_name} to S3.")
    except Exception as e:
        print(f"Error saving data {table_name}: {e}")
        raise

def cache_df(dataframe):
    """
    Cache a DataFrame and perform an action to ensure it is cached.

    Parameters:
    - dataframe (DataFrame): The DataFrame to be cached.

    Returns:
    - None
    """
    dataframe.cache()
    dataframe.count()
