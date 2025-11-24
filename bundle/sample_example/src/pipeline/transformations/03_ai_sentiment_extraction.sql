CREATE STREAMING TABLE ai_sentiment_extraction_game_reviews as 
select * , 
ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'Tell me if this review is about the game overall or specific to gameplay mechanics, music, script, character development, or something else. Be concise using only these topics as the key and the value as a json dictionary with the sentiment and sub category of the topic. Senitment value can only be positive, negative, or neutral. Return JSON ONLY. No markdown formats or tick marks. No other text outside the JSON. JSON format:
        {"overall": <sentiment if present else null>,
            "summary": <10 word or less concise summary of key points that affected sentiment>,
            "gameplay_mechanics": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "matchmaking_game_balance": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "game_performance": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "replayability": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "character": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "monetization": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "bugs_glitches_techissues": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "graphics_audio": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "suggestion_feedback": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "account_issues": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "cheating_hacking": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "toxicity": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "player_retention": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "onboarding": {"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>},
            "other": [{"topic": <topic>, "sentiment": <sentiment (positive, negative, or neutral)>}]
        }' || translated_review
    ) as review_topic
from STREAM(translated_game_reviews)
