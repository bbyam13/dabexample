-- Please edit the sample below

CREATE STREAMING TABLE
    translated_game_reviews
SELECT 
      *, 
      ai_translate(review, 'en') AS translated_review
  FROM STREAM(sampled_game_reviews)