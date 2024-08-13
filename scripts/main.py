# main.py
from functions import create_spark_session, read_data, process_netflix_data, process_amazon_data, save_data, cache_df
from pyspark.sql.functions import (
    col, 
    lit, 
)

def main():
    # Initialize spark
    spark = create_spark_session('desafio')

    bucket = "{nome_do_bucket}"

    # Read data
    netflix_df = read_data(spark, f"{bucket}/bronze/netflix/combined_data_*.txt")
    netflix_movie_title_df = read_data(spark, f"{bucket}/bronze/netflix/movie_titles.csv", format="csv", sep=",", header="false")
    amazon_df = read_data(spark, f"{bucket}/bronze/amazon", format="csv", sep="\t", header="true")

    # Cache data
    cache_df(netflix_df)
    cache_df(amazon_df)
    cache_df(netflix_movie_title_df)

    # Process Netflix data
    processed_netflix_df = process_netflix_data(netflix_df)

    # Process movie titles
    netflix_movie_title_df = (
        netflix_movie_title_df.withColumn("movie_id", col("_c0").cast("int"))
        .withColumn("year", col("_c1").cast("int"))
        .withColumn("title", col("_c2").cast("string"))
        .drop("_c0", "_c1", "_c2")
    )
    # Join dataframes
    netflix_final_df = (
        processed_netflix_df.alias('main')
        .join(netflix_movie_title_df.alias('movie'), on="movie_id", how="left")
        .select("main.customer_id", "main.movie_id", "main.ranking", "main.date", "movie.title", "movie.year")
        .withColumn("source", lit('netflix'))
    )

    # Process Amazon data
    amazon_final_df = process_amazon_data(amazon_df)

    # saving Dataframes in silver zone
    save_data(processed_netflix_df, "silver", "processed_netflix")
    save_data(netflix_movie_title_df, "silver", "netflix_movie_title")
    save_data(amazon_final_df, "silver", "amazon_final")

    # creating ratings dataframes
    ratings_df = netflix_final_df.union(amazon_final_df)

    # create DataFrame 'customer' 
    customer_df = ratings_df.select(col("customer_id").alias("id")).distinct()

    # create DataFrame 'movies' 
    movies_df = ratings_df.select(col("movie_id").alias("id"), "title").distinct()

    # saving Dataframes in gold zone
    save_data(customer_df, "gold", "customer")
    save_data(movies_df, "gold", "movies")
    save_data(ratings_df, "gold", "ratings")

if __name__ == "__main__":
    main()
