-- Drops stored procedures if they exist
DROP PROCEDURE IF EXISTS get_games_by_genre;
DROP PROCEDURE IF EXISTS get_average_rating_by_developer;
DROP PROCEDURE IF EXISTS get_oldest_games;
<<<<<<< HEAD
DROP PROCEDURE IF EXISTS top_rated_games;

=======
DROP PROCEDURE IF EXISTS get_top_rated_games;
>>>>>>> 51cf6a1922f35a07f2ab1a9171becbd938b129a9

-- Create a stored procedure for retrieving the total number of games by each genre
DELIMITER $$
CREATE PROCEDURE get_games_by_genre()
BEGIN
    SELECT genres.name as genre_name, COUNT(games.id) as game_count
    FROM games
    JOIN game_genres ON games.id = game_genres.game_id
    JOIN genres ON game_genres.genre_id = genres.id
    GROUP BY genres.name
    ORDER BY game_count DESC;
END $$
DELIMITER ;

-- Create a stored procedure for retrieving the average rating of games for the first 100 developers
DELIMITER $$
CREATE PROCEDURE get_average_rating_by_developer()
BEGIN
    SELECT developers.name as developer_name, AVG(games.rating) as average_rating
    FROM games
    JOIN game_developers ON games.id = game_developers.game_id
    JOIN developers ON game_developers.developer_id = developers.id
    GROUP BY developers.name
    ORDER BY average_rating DESC
    LIMIT 100;
END $$
DELIMITER ;

<<<<<<< HEAD
DELIMITER $$
=======
-- Create a stored procedure for retrieving  to oldest gmaes 

DELIMITER;;
>>>>>>> 51cf6a1922f35a07f2ab1a9171becbd938b129a9
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
End $$
DELIMITER ;

-- Create a stored procedure for top ten games by ratings
<<<<<<< HEAD
DELIMITER $$
CREATE PROCEDURE top_rated_games()
=======

DELIMITER //
CREATE PROCEDURE get_top_rated_games()
>>>>>>> 51cf6a1922f35a07f2ab1a9171becbd938b129a9
BEGIN
    SELECT g.name, g.rating, p.name AS platform, d.name AS developer, pu.name AS publisher, GROUP_CONCAT(DISTINCT g2.name SEPARATOR ', ') AS genre
    FROM games g
    LEFT JOIN game_platforms gp ON g.id = gp.game_id
    LEFT JOIN platforms p ON gp.platform_id = p.id
    LEFT JOIN game_developers gd ON g.id = gd.game_id
    LEFT JOIN developers d ON gd.developer_id = d.id
    LEFT JOIN game_publishers gpu ON g.id = gpu.game_id
    LEFT JOIN publishers pu ON gpu.publisher_id = pu.id
    LEFT JOIN game_genres gg ON g.id = gg.game_id
    LEFT JOIN genres g2 ON gg.genre_id = g2.id
    GROUP BY g.name
    ORDER BY g.rating DESC
    LIMIT 10;
END $$
DELIMITER ;

