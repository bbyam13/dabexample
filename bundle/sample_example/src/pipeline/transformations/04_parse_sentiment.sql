
CREATE STREAMING TABLE parsed_sentiment_game_reviews (
CONSTRAINT valid_overall_sentiment EXPECT (overall IS NOT NULL) ON VIOLATION DROP ROW)
AS
with review_agg as (
  select
    review_id,
    game_name,
    translated_review,
    review,
    from_json(
      review_topic,
      'STRUCT<overall: STRING, summary: STRING, gameplay_mechanics: STRUCT<sentiment: STRING, sub_topic: STRING>, matchmaking_game_balance: STRUCT<sentiment: STRING, sub_topic: STRING>, game_performance: STRUCT<sentiment: STRING, sub_topic: STRING>, replayability: STRUCT<sentiment: STRING, sub_topic: STRING>, character: STRUCT<sentiment: STRING, sub_topic: STRING>, monetization: STRUCT<sentiment: STRING, sub_topic: STRING>, bugs_glitches_techissues: STRUCT<sentiment: STRING, sub_topic: STRING>, graphics_audio: STRUCT<sentiment: STRING, sub_topic: STRING>, suggestion_feedback: STRUCT<sentiment: STRING, sub_topic: STRING>, account_issues: STRUCT<sentiment: STRING, sub_topic: STRING>, cheating_hacking: STRUCT<sentiment: STRING, sub_topic: STRING>, toxicity: STRUCT<sentiment: STRING, sub_topic: STRING>, player_retention: STRUCT<sentiment: STRING, sub_topic: STRING>, onboarding: STRUCT<sentiment: STRING, sub_topic: STRING>, other: ARRAY<STRUCT<topic: STRING, sentiment: STRING>>>'
    ) as review_data,
    comment_count,
    received_for_free,
    steam_purchase,
    timestamp_created,
    timestamp_updated,
    voted_up,
    votes_funny,
    votes_up,
    weighted_vote_score,
    written_during_early_access,
    author_steamid,
    author_num_reviews,
    author_num_games_owned,
    author_last_played,
    author_playtime_forever,
    author_playtime_at_review,
    author_playtime_at_last_two_weeks
  from
    STREAM(ai_sentiment_extraction_game_reviews)
)
select
  review_id,
  game_name,
  translated_review,
  review,
  review_data.overall as overall,
  review_data.summary as summary,
  review_data.gameplay_mechanics.sentiment as gameplay_mechanics,
  review_data.gameplay_mechanics.sub_topic as gameplay_mechanics_sub_topic,
  review_data.matchmaking_game_balance.sentiment as matchmaking_game_balance,
  review_data.matchmaking_game_balance.sub_topic as matchmaking_game_balance_sub_topic,
  review_data.game_performance.sentiment as game_performance,
  review_data.game_performance.sub_topic as game_performance_sub_topic,
  review_data.replayability.sentiment as replayability,
  review_data.replayability.sub_topic as replayability_sub_topic,
  review_data.character.sentiment as character,
  review_data.character.sub_topic as character_sub_topic,
  review_data.monetization.sentiment as monetization,
  review_data.monetization.sub_topic as monetization_sub_topic,
  review_data.bugs_glitches_techissues.sentiment as bugs_glitches_techissues,
  review_data.bugs_glitches_techissues.sub_topic as bugs_glitches_techissues_sub_topic,
  review_data.graphics_audio.sentiment as graphics_audio,
  review_data.graphics_audio.sub_topic as graphics_audio_sub_topic,
  review_data.suggestion_feedback.sentiment as suggestion_feedback,
  review_data.suggestion_feedback.sub_topic as suggestion_feedback_sub_topic,
  review_data.account_issues.sentiment as account_issues,
  review_data.account_issues.sub_topic as account_issues_sub_topic,
  review_data.cheating_hacking.sentiment as cheating_hacking,
  review_data.cheating_hacking.sub_topic as cheating_hacking_sub_topic,
  review_data.toxicity.sentiment as toxicity,
  review_data.toxicity.sub_topic as toxicity_sub_topic,
  review_data.player_retention.sentiment as player_retention,
  review_data.player_retention.sub_topic as player_retention_sub_topic,
  review_data.onboarding.sentiment as onboarding,
  review_data.onboarding.sub_topic as onboarding_sub_topic,
  review_data.other as other,
  comment_count,
  received_for_free,
  steam_purchase,
  timestamp_created,
  timestamp_updated,
  voted_up,
  votes_funny,
  votes_up,
  weighted_vote_score,
  written_during_early_access,
  author_steamid,
  author_num_reviews,
  author_num_games_owned,
  author_last_played,
  author_playtime_forever,
  author_playtime_at_review,
  author_playtime_at_last_two_weeks
from
  review_agg;



