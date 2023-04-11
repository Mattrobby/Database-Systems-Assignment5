-- Drops stored procedures if they exist
DROP PROCEDURE IF EXISTS get_games_by_genre;
DROP PROCEDURE IF EXISTS get_average_rating_by_developer;
DROP PROCEDURE IF EXISTS get_oldest_games()


-- Create a stored procedure for retrieving the total number of games by each genre
DELIMITER ;;
CREATE PROCEDURE get_games_by_genre()
BEGIN
    SELECT genres.name as genre_name, COUNT(games.id) as game_count
    FROM games
    JOIN game_genres ON games.id = game_genres.game_id
    JOIN genres ON game_genres.genre_id = genres.id
    GROUP BY genres.name
    ORDER BY game_count DESC;
END;;
DELIMITER ;

-- Create a stored procedure for retrieving the average rating of games for the first 100 developers
DELIMITER ;;
CREATE PROCEDURE get_average_rating_by_developer()
BEGIN
    SELECT developers.name as developer_name, AVG(games.rating) as average_rating
    FROM games
    JOIN game_developers ON games.id = game_developers.game_id
    JOIN developers ON game_developers.developer_id = developers.id
    GROUP BY developers.name
    ORDER BY average_rating DESC
    LIMIT 100;
END;;
DELIMITER ;

DELIMITER;;
CREATE PROCEDURE get_oldest_games()
BEGIN
        SELECT g.name AS game_name, g.released, p.name AS publisher_name, g.rating
        FROM (
                    SELECT game_id, MAX(publisher_id) AS publisher_id
                    FROM game_publishers
                    GROUP BY game_id
        ) gp
        INNER JOIN games g ON gp.game_id = g.id
        INNER JOIN publishers p ON gp.publisher_id = p.id
        WHERE g.released IS NOT NULL
        ORDER BY g.released ASC
        LIMIT 10;
End;;
DELIMITER;;
