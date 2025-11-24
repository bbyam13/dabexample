CREATE STREAMING TABLE game_reviews_gold AS
select  
  review_id,
  game_name,
  translated_review,
  review,
  overall as overall_sentiment,
  summary as ai_summary,
  comment_count,
  received_for_free,
  steam_purchase,
  timestamp_created,
  timestamp_updated,
  voted_up,
  votes_funny,
  votes_up,
  comment_count + votes_up + votes_funny as total_interactions,
  weighted_vote_score,
  written_during_early_access,
  author_steamid,
  author_num_reviews,
  author_num_games_owned,
  author_last_played,
  author_playtime_forever,
  author_playtime_at_review,
  author_playtime_at_last_two_weeks
from STREAM(parsed_sentiment_game_reviews);


CREATE STREAMING TABLE game_reviews_sentiment_gold AS

with other_topics_exploded as (
  select game_name, review_id, explode(other) as other_topic from STREAM(parsed_sentiment_game_reviews)
)

select * from (
SELECT
  game_name,
  review_id,
  -- pivot the topics, sentiement, and subtopics for easier analysis*/
  inline(arrays_zip(
    -- topic names
    array(
      'gameplay_mechanics',
      'matchmaking_game_balance',
      'game_performance',
      'replayability',
      'character',
      'monetization',
      'bugs_glitches_techissues',
      'graphics_audio',
      'suggestion_feedback',
      'account_issues',
      'cheating_hacking',
      'toxicity',
      'player_retention',
      'onboarding'
    ),
    -- topic sentiment columns
    array(
      gameplay_mechanics,
      matchmaking_game_balance,
      game_performance,
      replayability,
      character,
      monetization,
      bugs_glitches_techissues,
      graphics_audio,
      suggestion_feedback,
      account_issues,
      cheating_hacking,
      toxicity,
      player_retention,
      onboarding
    ),
    -- subtopic columns
    array(
      gameplay_mechanics_sub_topic,
      matchmaking_game_balance_sub_topic,
      game_performance_sub_topic,
      replayability_sub_topic,
      character_sub_topic,
      monetization_sub_topic,
      bugs_glitches_techissues_sub_topic,
      graphics_audio_sub_topic,
      suggestion_feedback_sub_topic,
      account_issues_sub_topic,
      cheating_hacking_sub_topic,
      toxicity_sub_topic,
      player_retention_sub_topic,
      onboarding_sub_topic
    )
  )) AS (category, sentiment, sub_category)
FROM STREAM(parsed_sentiment_game_reviews)) as x
where x.sentiment is not null

UNION ALL
-- union with exploded other topics
select game_name, review_id, "other" as other, other_topic.sentiment as sentiment, other_topic.topic as sub_category from other_topics_exploded;



