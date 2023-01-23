CREATE TABLE ratings(  
    user_id INT NOT NULL,
    movie_id INT NOT NULL,
    rating DECIMAL(2, 1) NOT NULL,
    created_ts DATETIME,
    PRIMARY KEY(user_id, movie_id)
) COMMENT '';