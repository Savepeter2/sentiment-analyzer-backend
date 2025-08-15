import logging
import os
import sys
import configparser
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import APIRouter, Body, Depends, status, Response, HTTPException, File, UploadFile
from configs.config import (
    USER_DIM_TABLE, TOPIC_DIM_TABLE, FIRM_DIM_TABLE,
    FACT_TWEETS_TABLE, DATE_DIM_TABLE,
    logger
)
from app.database import conn
import pandas as pd
import asyncio



router = APIRouter()


@router.get("/mentions", tags=["Mentions"])
async def mentions(
                        firm_dim_table: str = FIRM_DIM_TABLE,
                        tweet_fact_table: str = FACT_TWEETS_TABLE,
                        date_dim_table: str = DATE_DIM_TABLE

) -> dict:
    """
    Endpoint to retrieve (mentions and reach, sentiment analysis, most popular tweets) data for the mentions home page.
    
    This endpoint returns data for mentions and reach, sentiment analysis, and most popular tweets.
    
    Args:
        
        user_dim_table (str): Name of the user dimension table.
        
        topic_dim_table (str): Name of the topic dimension table.
        
        firm_dim_table (str): Name of the firm dimension table.
        
        tweet_fact_table (str): Name of the tweet fact table.
        
        date_dim_table (str): Name of the date dimension table.
    
    Returns:
        
        dict: A dictionary containing the status, message, and body with the following structure:
        
        - mentions_reach: List of dictionaries with firm, date, mentions, reach, and tweet count.
        
        - sentiment_analysis: List of dictionaries with firm, date, sentiment, sentiment count, sentiment percentage of day, and total tweets per day.
        
        - tweet_engagement: List of dictionaries with username, firm, sentiment, date difference, tweet text, followers formatted, followers count, no replies, no retweets, no likes, no views, total engagement, engagement rate, date, tweet id, and popularity score.
    
    Example:
    ```json
    {
        "status": "success",
        "message": "Mentions page data retrieved successfully",
        "body": {
            "mentions_reach": [
                {
                    "firm_id": 1,
                    "firm": "Firm A",
                    "date": "2024-01-01",
                    "year": 2024,
                    "month": 1,
                    "day": 1,
                    "mentions": 100,
                    "reach": 1000,
                    "tweet_count": 50
                }],
            "sentiment_analysis": [
                {
                    "firm": "Firm A",
                    "date": "2024-01-01",
                    "year": 2024,
                    "month": 1,
                    "day": 1,
                    "sentiment": "positive",
                    "sentiment_count": 30,
                    "sentiment_percentage_of_day": 60.0,
                    "total_tweets_per_day": 50
                }],
            "tweet_engagement": [
                {
                    "username": "user1",
                    "firm": "Firm A",
                    "sentiment": "positive",
                    "date_diff": "Today",
                    "tweet": "This is a tweet text",
                    "followers_formatted": "1K",
                    "followers_count": 1000,
                    "no_replies": 10,
                    "no_retweets": 5,
                    "no_likes": 20,
                    "no_views": 1000,
                    "total_engagement": 35,
                    "engagement": "3.5%",
                    "date": "2024-01-01",
                    "tweet_id": 123456789,
                    "popularity_score": 50
                }]
        }
    }
    """
    try:

            # Base optimized query using distribution and sort keys efficiently
            mentions_reach_query = """
            WITH firm_daily_metrics AS (
                SELECT 
                    tf.firm_id,
                    fd.firm,
                    dd.date,
                    dd.year,
                    dd.month,
                    dd.day,
                    SUM(tf.mentions) as daily_mentions,
                    SUM(tf.no_views) as daily_reach,
                    COUNT(tf.tweet_id) as daily_tweet_count
                FROM {tweet_fact_table} tf
                INNER JOIN {firm_dim_table} fd ON tf.firm_id = fd.firm_id
                INNER JOIN {date_dim_table} dd ON tf.date_id = dd.date_id
                GROUP BY 
                    tf.firm_id, 
                    fd.firm, 
                    dd.date, 
                    dd.year, 
                    dd.month, 
                    dd.day
            )
            SELECT 
                firm_id,
                firm,
                date,
                year,
                month,
                day,
                daily_mentions as mentions,
                daily_reach as reach,
                daily_tweet_count as tweet_count
            FROM firm_daily_metrics
            ORDER BY firm, date;
            """
        
            mentions_reach_query = mentions_reach_query.format(
                tweet_fact_table=tweet_fact_table,
                firm_dim_table=firm_dim_table,
                date_dim_table=date_dim_table
            )

            sentiment_analysis_query = """

                            WITH firm_daily_sentiment AS (
                    SELECT 
                        tf.firm_id,
                        fd.firm,
                        dd.date,
                        dd.year,
                        dd.month,
                        dd.day,
                        tf.consensus_sentiment as sentiment,
                        COUNT(*) as sentiment_count
                    FROM tweet_fact tf
                    INNER JOIN firm_dim fd ON tf.firm_id = fd.firm_id  -- Join on distribution key
                    INNER JOIN date_dim dd ON tf.date_id = dd.date_id  -- Join on sort key
                    WHERE tf.consensus_sentiment IS NOT NULL
                        AND tf.consensus_sentiment != ''
                        -- Optional: Add date range filter
                        -- AND dd.date >= '2024-01-01'
                        -- AND dd.date <= '2024-12-31'
                    GROUP BY 
                        tf.firm_id,
                        fd.firm,
                        dd.date,
                        dd.year,
                        dd.month,
                        dd.day,
                        tf.consensus_sentiment
                )
                SELECT 
                    firm,
                    date,
                    year,
                    month,
                    day,
                    sentiment,
                    sentiment_count,
                    -- Additional useful metrics
                    ROUND(
                        (sentiment_count * 100.0) / 
                        SUM(sentiment_count) OVER (PARTITION BY firm, date), 
                        2
                    ) as sentiment_percentage_of_day,
                    SUM(sentiment_count) OVER (PARTITION BY firm, date) as total_tweets_per_day
                FROM firm_daily_sentiment
                ORDER BY 
                    firm,
                    date,
                    sentiment_count DESC;


            """


            tweet_engagement_query = """
            
                WITH tweet_engagement AS (
                    SELECT 
                        tf.tweet_id,
                        ud.username,
                        fd.firm,
                        tf.consensus_sentiment as sentiment,
                        dd.date,
                        tf.tweet_text as tweet,
                        ud.followers_count,
                        tf.no_replies,
                        tf.no_retweets,
                        tf.no_likes,
                        tf.no_views,
                        tf.no_quotes,
                        tf.no_bookmarks,
                        -- Calculate total engagement
                        (tf.no_replies + tf.no_retweets + tf.no_likes + tf.no_quotes + tf.no_bookmarks) as total_engagement,
                        -- Calculate engagement rate as percentage
                        CASE 
                            WHEN tf.no_views > 0 THEN 
                                ROUND(((tf.no_replies + tf.no_retweets + tf.no_likes + tf.no_quotes + tf.no_bookmarks) * 100.0) / tf.no_views, 2)
                            ELSE 0 
                        END as engagement_rate,
                        -- Calculate date difference from today
                        DATEDIFF(day, dd.date, CURRENT_DATE) as days_ago
                    FROM tweet_fact tf
                    INNER JOIN user_dim ud ON tf.user_id = ud.user_id        -- Join on distribution key
                    INNER JOIN firm_dim fd ON tf.firm_id = fd.firm_id        -- Join on distribution key
                    INNER JOIN date_dim dd ON tf.date_id = dd.date_id        -- Join on sort key
                    WHERE tf.consensus_sentiment IS NOT NULL
                        AND tf.consensus_sentiment != ''
                        AND tf.tweet_text IS NOT NULL
                        AND tf.tweet_text != ''
                        -- Optional: Filter by date range (uncomment and adjust as needed)
                        -- AND dd.date >= DATEADD(day, -90, CURRENT_DATE)  -- Last 90 days
                        -- Optional: Filter by minimum engagement (uncomment to filter low engagement tweets)
                        -- AND (tf.no_replies + tf.no_retweets + tf.no_likes + tf.no_quotes + tf.no_bookmarks) >= 5
                )
                SELECT 
                    username,
                    firm,
                    sentiment,
                    -- Format date difference in human readable format
                    CASE 
                        WHEN days_ago = 0 THEN 'Today'
                        WHEN days_ago = 1 THEN '1 day ago'
                        WHEN days_ago < 7 THEN days_ago || ' days ago'
                        WHEN days_ago < 14 THEN 'about 1 week ago'
                        WHEN days_ago < 21 THEN 'about 2 weeks ago'
                        WHEN days_ago < 30 THEN 'about 3 weeks ago'
                        WHEN days_ago < 60 THEN 'about 1 month ago'
                        WHEN days_ago < 90 THEN 'about 2 months ago'
                        WHEN days_ago < 120 THEN 'about 3 months ago'
                        WHEN days_ago < 365 THEN 'about ' || ROUND(days_ago/30.0, 0) || ' months ago'
                        ELSE 'about ' || ROUND(days_ago/365.0, 0) || ' years ago'
                    END as date_diff,
                    -- Truncate tweet text if too long (optional)
                    CASE 
                        WHEN LENGTH(tweet) > 200 THEN LEFT(tweet, 197) || '...'
                        ELSE tweet 
                    END as tweet,
                    -- Format follower count in human readable format
                    CASE 
                        WHEN followers_count >= 1000000 THEN ROUND(followers_count/1000000.0, 1) || 'M'
                        WHEN followers_count >= 1000 THEN ROUND(followers_count/1000.0, 1) || 'K'
                        ELSE followers_count::VARCHAR
                    END as followers_formatted,
                    followers_count,
                    no_replies,
                    no_retweets, 
                    no_likes,
                    no_views,
                    total_engagement,
                    engagement_rate || '%' as engagement,
                    -- Additional metrics for sorting and analysis
                    date,
                    tweet_id,
                    -- Popularity score (weighted combination of engagement metrics)
                    (no_likes * 1.0 + no_retweets * 2.0 + no_replies * 1.5 + no_quotes * 2.5 + no_bookmarks * 1.5) as popularity_score
                FROM tweet_engagement
                -- Sort by most popular tweets first
                ORDER BY 
                    total_engagement DESC,
                    engagement_rate DESC,
                    no_views DESC,
                    popularity_score DESC
                            """

            mentions_reach_df = pd.read_sql_query(mentions_reach_query, conn)
            sentiment_analysis_df = pd.read_sql_query(sentiment_analysis_query, conn)
            tweet_eng_df = pd.read_sql_query(tweet_engagement_query, conn)

            if not (tweet_eng_df.empty and mentions_reach_df.empty and sentiment_analysis_df.empty):
        
                mentions_reach_result = mentions_reach_df.to_dict(orient="records")
                sentiment_result = sentiment_analysis_df.to_dict(orient="records")
                tweet_eng_result = tweet_eng_df.to_dict(orient="records")

                logger.info(f"Mentions and reach data retrieved successfully:")
                x = {
                    "status": "success",
                    "message": "Mentions page data retrieved successfully",
                    "body": {
                        "mentions_reach": mentions_reach_result,
                        "sentiment_analysis": sentiment_result,
                        "tweet_engagement": tweet_eng_result
                    }
                }
                print("x:", x)
                return x
            
            else:
                return {
                    "status": "success",
                    "message": "No data found",
                    "body": []
                }


    except Exception as e:
        if not isinstance(e, HTTPException):
            logger.error(f"An error occured while retrieving mentions and reach data: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "status": "error",
                    "message": "An error occured while retrieving mentions and reach data",
                    "body": str(e)
                }
            )
        else:
            raise e


