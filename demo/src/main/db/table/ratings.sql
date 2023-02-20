-- Active: 1674446945536@@localhost@3306@test_db
CREATE TABLE ratings(
    id BIGINT AUTO_INCREMENT,  
    user_id BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    rating DECIMAL(2, 1) NOT NULL,
    created_ts DATETIME,
    PRIMARY KEY(id)
) COMMENT ''
PARTITION BY KEY (id) PARTITIONS 10;

ALTER TABLE ratings ADD index (`movie_id`);