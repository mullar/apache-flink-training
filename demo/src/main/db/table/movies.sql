-- Active: 1674446945536@@localhost@3306@test_db
CREATE TABLE movies(  
    movie_id BIGINT NOT NULL PRIMARY KEY,
    title varchar(256),
    genres VARCHAR(256)
) COMMENT '';