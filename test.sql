-- Active: 1674446945536@@localhost@3306@test_db

select MAX(movie_id) from movies;

SELECT user_id,movie_id,rating,created_ts FROM ratings;

select COUNT(*) from ratings;

SELECT movie_id,title,average_rating FROM top_rated_movies;

select count(*) from top_rated_movies;

show PROCESSLIST;
SELECT count(*) FROM `ratings`;

SET GLOBAL innodb_buffer_pool_size=536870912;

	
SELECT count(*) FROM `ratings` WHERE `movie_id` BETWEEN  72381 AND 72400;

CREATE OR REPLACE TABLE ratings (movie_id BIGINT)
  PARTITION BY KEY (movie_id)
  PARTITIONS 100;


SELECT movie_id, COUNT(rating) AS total
FROM ratings
GROUP BY movie_id
HAVING total > 0
ORDER BY total DESC;

SELECT `movie_id`, `rating` FROM `ratings` WHERE `movie_id` = 201250;


SELECT `movie_id`, `rating` FROM `ratings` WHERE `movie_id` BETWEEN 51 AND 53;