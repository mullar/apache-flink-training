CREATE TABLE ratings(  
    user_id INT NOT NULL,
    movie_id INT NOT NULL,
    rating DECIMAL(2, 1),
    timestamp DATETIME,
    UNIQUE(user_id, movie_id)
) COMMENT '';