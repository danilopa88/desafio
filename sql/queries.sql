--1. Quantos filmes estão disponíveis na Amazon? E na Netflix?

SELECT source, COUNT(DISTINCT title) AS movie_count
FROM "desafio"."ratings"
GROUP BY source;

--2. Dos filmes disponíveis na Amazon, quantos % estão disponíveis na Netflix?

WITH amazon_movies AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
),
netflix_movies AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
),
amazon_count AS (
  SELECT COUNT(*) AS amazon_total
  FROM amazon_movies
),
netflix_count AS (
  SELECT COUNT(*) AS netflix_common
  FROM amazon_movies
  JOIN netflix_movies
  ON amazon_movies.title = netflix_movies.title
)
SELECT 
    ROUND((CAST(netflix_common AS DOUBLE) / amazon_total) * 100, 2) AS percent_available_on_netflix
FROM amazon_count
CROSS JOIN netflix_count;


--3. O quão perto a média das notas dos filmes disponíveis na Amazon está dos filmes disponíveis na Netflix?

WITH amazon_ratings AS (
  SELECT title, AVG(ranking) AS avg_rating
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
  GROUP BY title
),
netflix_ratings AS (
  SELECT title, AVG(ranking) AS avg_rating
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
  GROUP BY title
),
amazon_avg AS (
  SELECT AVG(avg_rating) AS avg_amazon
  FROM amazon_ratings
),
netflix_avg AS (
  SELECT AVG(avg_rating) AS avg_netflix
  FROM netflix_ratings
)
SELECT ABS(avg_amazon - avg_netflix) AS difference
FROM amazon_avg
CROSS JOIN netflix_avg;


--4. Qual ano de lançamento possui mais filmes na Netflix?

SELECT year, COUNT(DISTINCT title) AS film_count
FROM "desafio"."ratings"
WHERE source = 'netflix'
GROUP BY year
ORDER BY film_count DESC
LIMIT 1;

--5. Quais filmes que não estão disponíveis no catálogo da Amazon foram melhor avaliados? (Melhores notas são as 4 e 5)

WITH amazon_titles AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
),
highly_rated_movies AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE ranking IN (4, 5)
)
SELECT DISTINCT title
FROM highly_rated_movies
WHERE title NOT IN (SELECT title FROM amazon_titles);

--6. Quais filmes que não estão disponíveis no catálogo da Netflix foram melhor avaliados? (Melhores notas são as 4 e 5)

WITH netflix_titles AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
),
highly_rated_movies AS (
  SELECT DISTINCT title
  FROM "desafio"."ratings"
  WHERE ranking IN (4, 5)
)
SELECT title
FROM highly_rated_movies
WHERE title NOT IN (SELECT title FROM netflix_titles);


--7. Quais os 10 filmes que possuem mais avaliações nas plataformas?

SELECT title, COUNT(*) AS review_count
FROM "desafio"."ratings"
GROUP BY title
ORDER BY review_count DESC
LIMIT 10;


--8. Quais são os 5 clientes que mais avaliaram filmes na Amazon e quantos produtos diferentes eles avaliaram? E na Netflix?

WITH amazon_reviews AS (
  SELECT customer_id, COUNT(DISTINCT title) AS product_count
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
  GROUP BY customer_id
  ORDER BY product_count DESC
  LIMIT 5
),
netflix_reviews AS (
  SELECT customer_id, COUNT(DISTINCT title) AS product_count
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
  GROUP BY customer_id
  ORDER BY product_count DESC
  LIMIT 5
)
SELECT 'Amazon' AS platform, customer_id, product_count
FROM amazon_reviews
UNION ALL
SELECT 'Netflix' AS platform, customer_id, product_count
FROM netflix_reviews;


--9. Quantos filmes foram avaliados na data de avaliação mais recente na Amazon?

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
),
recent_movies AS (
  SELECT COUNT(DISTINCT title) AS recent_film_count
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
  AND date = (SELECT max_date FROM latest_date)
)
SELECT recent_film_count
FROM recent_movies;

--10. Quais os 10 filmes mais bem avaliados nesta data?

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
),
ratings_recent AS (
  SELECT title, ranking
  FROM "desafio"."ratings"
  WHERE source = 'amazon'
  AND date = (SELECT max_date FROM latest_date)
),
weighted_sum AS (
  SELECT
    title,
    SUM(ranking * 1) AS total_weighted_ranking,  
    COUNT(*) AS total_count 
  FROM ratings_recent
  GROUP BY title
),
weighted_avg AS (
  SELECT
    title,
    total_weighted_ranking,
    total_count,
    (CAST(total_weighted_ranking AS DECIMAL(10,2)) / CAST(total_count AS DECIMAL(10,2))) AS weighted_average
  FROM weighted_sum
)
SELECT
  title,
  total_weighted_ranking,
  total_count,
  weighted_average as weighted_average
FROM weighted_avg
WHERE weighted_average >= 4.50
ORDER BY total_weighted_ranking DESC, weighted_average DESC
LIMIT 10;


--11. Quantos filmes foram avaliados na data de avaliação mais recente na Netflix?

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
),
recent_movies AS (
  SELECT COUNT(DISTINCT title) AS recent_film_count
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
  AND date = (SELECT max_date FROM latest_date)
)
SELECT recent_film_count
FROM recent_movies;

--12. Quais os 10 filmes mais bem avaliados nesta data?

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
),
ratings_recent AS (
  SELECT title, ranking
  FROM "desafio"."ratings"
  WHERE source = 'netflix'
  AND date = (SELECT max_date FROM latest_date)
),
weighted_sum AS (
  SELECT
    title,
    SUM(ranking * 1) AS total_weighted_ranking,  
    COUNT(*) AS total_count 
  FROM ratings_recent
  GROUP BY title
),
weighted_avg AS (
  SELECT
    title,
    total_weighted_ranking,
    total_count,
    (CAST(total_weighted_ranking AS DECIMAL(10,2)) / CAST(total_count AS DECIMAL(10,2))) AS weighted_average
  FROM weighted_sum
)
SELECT
  title,
  total_weighted_ranking,
  total_count,
  weighted_average as weighted_average
FROM weighted_avg
WHERE weighted_average >= 4.50
ORDER BY total_weighted_ranking DESC, weighted_average DESC
LIMIT 10;





