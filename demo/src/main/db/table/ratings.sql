-- Active: 1674446945536@@localhost@3306@test_db
CREATE TABLE ratings(  
    user_id BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    rating DECIMAL(2, 1) NOT NULL,
    created_ts DATETIME,
    PRIMARY KEY(user_id, movie_id)
) COMMENT ''
PARTITION BY KEY (movie_id) PARTITIONS 100;