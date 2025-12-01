-- Create a table in Athena
CREATE EXTERNAL TABLE IF NOT EXISTS tmdb_movie_database.movie_data (
  adult STRING,
  budget BIGINT,
  genres STRING,
  homepage STRING,
  imdb_id STRING,
  keywords STRING,
  movie_id BIGINT,
  original_language STRING,
  overview STRING,
  popularity DOUBLE,
  popularity_category STRING,
  poster_url STRING,
  production_companies STRING,
  profit BIGINT,
  release_date DATE,
  release_year INT,
  revenue BIGINT,
  roi DOUBLE,
  runtime INT,
  spoken_languages STRING,
  status STRING,
  tagline STRING,
  title STRING,
  vote_average DOUBLE,
  vote_count INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://2025tmdbmoviedata/daily_outputs/'
TBLPROPERTIES ('classification' = 'csv');

-- Revenue Trends by Year
SELECT release_year,
       COUNT(*) AS movies,
       FORMAT('%,.2f', AVG(revenue)) AS avg_revenue
FROM movie_database.movie_data
WHERE release_year IS NOT NULL AND revenue != 0.00
GROUP BY release_year
ORDER BY release_year;

-- Top 10 Most Profitable Movies
SELECT DISTINCT title, revenue, budget, profit
FROM movie_database.movie_data
WHERE revenue IS NOT NULL AND budget IS NOT NULL
  AND CAST(budget AS DOUBLE) > 0
ORDER BY profit DESC
LIMIT 10;

-- ROI Analysis by Genre
SELECT genres,
       COUNT(*) AS movie_count,
       AVG(roi) AS avg_roi,
       AVG(profit) AS avg_profit
FROM movie_database.movie_data
WHERE genres != 'Unknown' AND roi IS NOT NULL
GROUP BY genres
ORDER BY avg_roi DESC;
