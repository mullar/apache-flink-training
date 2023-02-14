-- Active: 1674446945536@@localhost@3306@test_db
CREATE TABLE top_rated_movies(  
    movie_id BIGINT NOT NULL PRIMARY KEY,
    title varchar(256),
    average_rating DECIMAL(2, 1) NOT NULL
) COMMENT '';