import os 
import sys
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import APIRouter, HTTPException
from configs.config import (
    logger
)
from helpers.utils import generate_chatbot_response
from app.database import conn
import pandas as pd
import asyncio
from pydantic import BaseModel, Field
from typing import Dict, Any

router = APIRouter()

@router.get("/recommendations", tags=["Recommender"])
async def recommendation(
) -> dict:
    """
    Endpoint to retrieve Most Frequently discussed topics on KPI data for the recommender page.

    This Endpoint returns all required fields and their respective values for the Most Frequently Discussed Topics.

    Args:
        None
    
    Returns:
        
        dict: A dictionary containing the status, message, and body with the following structure:
        
        - recommendation_kpis: List of dictionaries with the following keys:
            - topic_name: Name of the topic
            - firm: Name of the firm
            - topic_id: ID of the topic
            - firm_id: ID of the firm
            - modeled_topics: Modeled topics
            - total_mentions_formatted: Formatted total mentions (e.g., "1K", "2M")
            - total_mentions: Total number of mentions
            - total_tweets_formatted: Formatted total tweets (e.g., "1K", "2M")
            - total_tweets: Total number of tweets
            - total_reach_formatted: Formatted total reach (e.g., "1K", "2M")
            - total_reach: Total reach value
            - unique_users: Number of unique users discussing the topic
            - active_days: Number of active days discussing the topic
            - first_discussion_date: Date of the first discussion
            - last_discussion_date: Date of the last discussion
            - avg_mentions_per_tweet: Average mentions per tweet
            - avg_reach_per_tweet: Average reach per tweet
            - positive_percent: Percentage of positive mentions
            - negative_percent: Percentage of negative mentions
            - neutral_percent: Percentage of neutral mentions
            - positive_sentiment_formatted: Formatted positive sentiment with percentage (e.g., "500 (50%)")
            - negative_sentiment_formatted: Formatted negative sentiment with percentage (e.g., "300 (30%)")
            - neutral_sentiment_formatted: Formatted neutral sentiment with percentage (e.g., "200 (20%)")
            - topic_score: Topic engagement score (combination of mentions, reach, and engagement)
    
    Example:
    ```json
    {
        "status": "success",
        "message": "Most frequent topics data retrieved successfully.",
        "body": {
            "recommendation_kpis": [{
                "topic_name": "Topic A",
                "firm": "Firm A",
                "topic_id": 1,
                "firm_id": 1,   
                "modeled_topics": "Modeled Topic A",
                "total_mentions_formatted": "1K",
                "total_mentions": 1000,
                "total_reach_formatted": "10K",
                "total_reach": 10000,
                "unique_users": 100,
                "active_days": 30,
                "first_discussion_date": "2024-01-01",
                "last_discussion_date": "2024-01-31",
                "avg_mentions_per_tweet": 2.0,
                "avg_reach_per_tweet": 20.0,
                "positive_percent": 50.0,
                "negative_percent": 30.0,
                "neutral_percent": 20.0,
                "positive_sentiment_formatted": "500 (50%)",
                "negative_sentiment_formatted": "300 (30%)",
                "neutral_sentiment_formatted": "200 (20%)",
                "topic_score": 150.0
            }]
        }
    }
    ```
    """
    try:
        most_frequent_topics_query = """

             WITH topic_analytics AS (
                    SELECT 
                        td.topic_id,
                        td.topic_name,
                        fd.firm,
                        tf.firm_id,
                        -- Topic metrics aggregation by firm
                        COUNT(tf.tweet_id) as total_mentions,  
                        COUNT(DISTINCT tf.user_id) as unique_users,
                        SUM(COALESCE(tf.no_views, 0)) as total_reach,
                        SUM(COALESCE(tf.no_likes + tf.no_retweets + tf.no_replies + tf.no_quotes + tf.no_bookmarks, 0)) as total_engagement,
                        -- Date range for topic activity
                        MIN(dd.date) as first_discussion_date,
                        MAX(dd.date) as last_discussion_date,
                        COUNT(DISTINCT dd.date) as active_days,
                        -- Average metrics
                        ROUND(AVG(COALESCE(tf.no_views, 0)), 2) as avg_reach_per_tweet,
                        -- Sentiment breakdown
                        SUM(CASE WHEN tf.gpt3_sentiment = 'Positive' THEN 1 ELSE 0 END) as positive_tweets,
                        SUM(CASE WHEN tf.gpt3_sentiment = 'Negative' THEN 1 ELSE 0 END) as negative_tweets,
                        SUM(CASE WHEN tf.gpt3_sentiment = 'Neutral' THEN 1 ELSE 0 END) as neutral_tweets
                    FROM tweet_fact tf
                    LEFT JOIN topic_dim td ON tf.topic_id = td.topic_id    -- Join on distribution key
                    LEFT JOIN firm_dim fd ON tf.firm_id = fd.firm_id       -- Join on distribution key
                    LEFT JOIN date_dim dd ON tf.date_id = dd.date_id       -- Join on sort key
                    WHERE td.topic_name IS NOT NULL
                        AND td.topic_name != ''
                        AND fd.firm IS NOT NULL
                        AND fd.firm != ''
                        -- Optional: Add date range filter
                        -- AND dd.date >= '2024-01-01'
                        -- AND dd.date <= '2024-12-31'
                        -- Optional: Filter by specific topics
                        -- AND td.topic_name ILIKE '%network%' OR td.topic_name ILIKE '%service%'
                        -- Optional: Filter by specific firms
                        -- AND fd.firm IN ('MTN', 'Airtel', 'Glo', '9mobile')
                    GROUP BY 
                        td.topic_id,
                        td.topic_name,
                        fd.firm,
                        tf.firm_id
                )
                SELECT 
                    topic_name,
                    firm,
                    topic_id,
                    firm_id,
                    -- Format total mentions (which is count of tweets about this topic)
                    CASE 
                        WHEN total_mentions >= 1000000 THEN ROUND(total_mentions/1000000.0, 1) || 'M'
                        WHEN total_mentions >= 1000 THEN ROUND(total_mentions/1000.0, 1) || 'K'
                        ELSE total_mentions::VARCHAR
                    END as total_mentions_formatted,
                    total_mentions,
                    -- Format total reach in readable format
                    CASE 
                        WHEN total_reach >= 1000000 THEN ROUND(total_reach/1000000.0, 1) || 'M'
                        WHEN total_reach >= 1000 THEN ROUND(total_reach/1000.0, 1) || 'K'
                        ELSE total_reach::VARCHAR
                    END as total_reach_formatted,
                    total_reach,
                    unique_users,
                    active_days,
                    first_discussion_date,
                    last_discussion_date,
                    avg_reach_per_tweet,
                    -- Sentiment percentages
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((positive_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END as positive_percent,
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((negative_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END as negative_percent,
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((neutral_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END as neutral_percent,
                    -- Formatted sentiment counts with percentages
                    positive_tweets || ' (' || 
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((positive_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END || '%)' as positive_sentiment_formatted,
                    
                    negative_tweets || ' (' || 
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((negative_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END || '%)' as negative_sentiment_formatted,
                    
                    neutral_tweets || ' (' || 
                    CASE 
                        WHEN total_mentions > 0 THEN ROUND((neutral_tweets * 100.0) / total_mentions, 1)
                        ELSE 0 
                    END || '%)' as neutral_sentiment_formatted,
                    -- Topic engagement score (combination of mentions, reach, and engagement)
                    ROUND((total_mentions * 0.5 + (total_reach/1000) * 0.3 + (total_engagement/100) * 0.2), 2) as topic_score
                FROM topic_analytics
                -- Sort by most frequent topics (highest mentions = most discussed topics) by firm
                ORDER BY 
                    total_mentions DESC,
                    total_reach DESC,
                    firm,
                    topic_name

                """
        
        frequent_topics_df = pd.read_sql_query(most_frequent_topics_query, conn)

        if not frequent_topics_df.empty:
            recommendation_kpis_result = frequent_topics_df.to_dict(orient="records")
            logger.info("Successfully retrieved most frequent topics data.")

            return {
                "status": "success",
                "message": "Most frequent topics data retrieved successfully.",
                "body": {
                    "recommendation_kpis": recommendation_kpis_result
                }
            }
        
        else:
            
            logger.warning("No data found for most frequent topics.")
            
            return {
                "status": "warning",
                "message": "No data found for most frequent topics.",
                "body": {
                    "recommendation_kpis": []
                }
            }
    except Exception as e:
        logger.error(f"Error retrieving most frequent topics data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": "Failed to retrieve most frequent topics data.",
                "body": str(e)
            }
        )


# Pydantic model for the request body
class ChatbotRequest(BaseModel):
    user_query: str = Field(
        ..., 
        description="The user's natural language query or question for the recommender chatbot",
        min_length=1,
        max_length=1000,
        example="What are some good sci-fi movies similar to Blade Runner?"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_query": "I'm looking for romantic comedies from the 2000s that are highly rated. Can you recommend some?"
            }
        }

# Pydantic model for the response
class ChatbotResponse(BaseModel):
    status: str
    message: str
    body: Any = None

@router.post("/recommendations/chatbot", 
             tags=["Recommender"],
             response_model=ChatbotResponse,
             summary="Get AI-Powered Content Recommendations",
             description="""
             Generate personalized content recommendations using AI chatbot
             
             This endpoint provides an intelligent conversational interface for getting personalized 
             content recommendations. The AI chatbot can understand natural language queries and 
             provide tailored suggestions based on user preferences.
             
             Features:
             - Natural language processing for user queries
             - Personalized recommendation generation
             - Context-aware responses
             - Support for various content types and preferences
             
             Example queries:
             - "I want to watch something funny tonight"
             - "Recommend sci-fi movies similar to Interstellar"
             - "What are good books for someone who likes fantasy?"
             - "Show me highly rated documentaries about nature"
             
             Response includes:
             - Status of the operation (success/warning/error)
             - Descriptive message about the result
             - Generated recommendations in the response body
             
             Note: The chatbot learns from the context of your query to provide 
             the most relevant recommendations possible.
             """,
             responses={
                 200: {
                     "description": "Successful recommendation generation",
                     "content": {
                         "application/json": {
                             "example": {
                                 "status": "success",
                                 "message": "Recommender Chatbot response generated successfully.",
                                 "body": "Based on your interest in sci-fi movies like Blade Runner, I recommend: 1) Ghost in the Shell (1995) - explores similar themes of identity and consciousness, 2) Ex Machina (2014) - focuses on AI and what makes us human..."
                             }
                         }
                     }
                 },
                 500: {
                     "description": "Internal server error during recommendation generation",
                     "content": {
                         "application/json": {
                             "example": {
                                 "detail": {
                                     "status": "error",
                                     "message": "Failed to generate chatbot response.",
                                     "body": "Connection timeout to recommendation service"
                                 }
                             }
                         }
                     }
                 }
             })
async def call_generate_chatbot_response(request: ChatbotRequest) -> Dict[str, Any]:
    try:
        response = generate_chatbot_response(request.user_query)
        print("response:", response)
        
        if response:
            logger.info("Recommender Chatbot response generated successfully.")
            return {
                "status": "success",
                "message": "Recommender Chatbot response generated successfully.",
                "body": response
            }
        else:
            logger.warning("Failed to generate recommender response for the given query.")
            return {
                "status": "warning",
                "message": "Failed to generate recommender response for the given query.",
                "body": None
            }
            
    except Exception as e:
        logger.error(f"Error generating chatbot response: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": "Failed to generate chatbot response.",
                "body": str(e)
            }
        )






