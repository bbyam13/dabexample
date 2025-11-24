CREATE STREAMING TABLE sampled_game_reviews AS
WITH game_reviews AS (
  SELECT
    *,
    concat_ws('_', uuid(), game_name) AS review_id
  FROM
    STREAM (bbyam_demo.player_feedback.steam_reviews_bronze)
),
popular_games AS (
  SELECT
    game_name
  FROM
    bbyam_demo.player_feedback.steam_reviews_bronze
  GROUP BY
    game_name
  HAVING
    COUNT(*) > 2000
)
--sample reviews from games with more than 2000 reviews
SELECT
  *
FROM
  game_reviews
WHERE
  (game_name IN (SELECT game_name FROM popular_games)
    AND abs(hash(review_id)) % 5 = 0)
  OR game_name NOT IN (
    SELECT game_name FROM popular_games
  );