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


@router.get("/analysis", tags=["Analysis"])
async def analysis(
) -> dict:
    
    """
    Endpoint to retrieve 
    
    (Overview KPIs, Sentiment Distribution, Monthly trends, Mentions vs Reach chart, Sentiment Comparison, Top Influencers by Mentions, Top Profiles) data for the analysis home page.
    
    This Endpoint returns all required fields and their respective values for the analysis home page.

    Args:
        None

    Returns:

        dict: A dictionary containing the status, message, and body with the following structure:

        - overview_kpis: Dictionary with total tweets, total users, total topics, and total firms.

        - top_influencers_by_mentions: List of dictionaries with username, firm, total mentions, and followers count.

        - top_profiles: username, firm, total tweets, followers count, and engagement rate.

    Example:
    ```json
    {
        "status": "success",
        "message": "Analysis home data retrieved successfully",
        "body": {
            "overview_kpis": [{
                "firm": "Firm A",
                "firm_id": 1,
                "date": "2024-01-01",
                "day": 1,
                "month": 1,
                "year": 2024,
                "total_mentions_formatted": "1K",
                "total_mentions": 1000,
                "total_reach_formatted": "10K",
                "total_reach": 10000,
                "positive_count": 500,
                "positive_percent": 50,
                "negative_count": 300,
                "negative_percent": 30,
                "neutral_count": 200,
                "neutral_percent": 20,
                "sentiment_total": 1000,
                "positive_sentiment_formatted": "500 (50%)",
                "negative_sentiment_formatted": "300 (30%)",
                "neutral_sentiment_formatted": "200 (20%)"
            }],
            "top_influencers_by_mentions": [{
                    "username": "influencer1",
                    "total_mentions_formatted": "1K",
                    "total_mentions": 1000,
                    "total_reach_formatted": "10K",
                    "total_reach": 10000,
                    "date": "2024-01-01",
                    "day": 1,
                    "month": 1,
                    "year": 2024,
                    "firm": "Firm A",
                    "daily_tweets": 50,
                    "avg_mentions_per_tweet": 20.0,
                    "mention_rate_percent": 10.0
                }
            ],
            "top_profiles": [{
                "username": "profile1",
                "firm": "Firm A",
                "date": "2024-01-01",
                "day": 1,
                "month": 1,
                "year": 2024,
                "followers_formatted": "1K",
                "followers": 1000,
                "engagement": 5.0,
                "posts_count": 10,
                "total_engagement": 50,
                "daily_reach": 10000,
                "avg_engagement_per_post": 5.0
            }]
        }
    }
    ```
        }
"""
    try:
        overview_kpis = """

        WITH firm_daily_analytics AS (
            SELECT 
                tf.firm_id,
                fd.firm,
                dd.date,
                dd.day,
                dd.month,
                dd.year,
                -- Total metrics
                SUM(tf.mentions) as total_mentions,
                SUM(tf.no_views) as total_reach,
                COUNT(*) as total_tweets,
                -- Sentiment counts
                SUM(CASE WHEN tf.consensus_sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN tf.consensus_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
                SUM(CASE WHEN tf.consensus_sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
                -- Count tweets with valid sentiment
                SUM(CASE WHEN tf.consensus_sentiment IN ('positive', 'negative', 'neutral') THEN 1 ELSE 0 END) as sentiment_total
            FROM tweet_fact tf
            INNER JOIN firm_dim fd ON tf.firm_id = fd.firm_id    -- Join on distribution key
            INNER JOIN date_dim dd ON tf.date_id = dd.date_id    -- Join on sort key
            WHERE tf.consensus_sentiment IS NOT NULL
                AND tf.consensus_sentiment != ''
                -- Optional: Add date range filter
                -- AND dd.date >= '2024-01-01'
                -- AND dd.date <= '2024-12-31'
            GROUP BY 
                tf.firm_id,
                fd.firm,
                dd.date,
                dd.day,
                dd.month,
                dd.year
        )
        SELECT 
            firm,
            firm_id,
            date,
            day,
            month,
            year,
            -- Format total mentions in readable format
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
            -- Sentiment counts and percentages
            positive_count,
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((positive_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END as positive_percent,
            negative_count,
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((negative_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END as negative_percent,
            neutral_count,
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((neutral_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END as neutral_percent,
            sentiment_total,
            -- Formatted sentiment with percentages (like your example)
            positive_count || ' (' || 
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((positive_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END || '%)' as positive_sentiment_formatted,
            
            negative_count || ' (' || 
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((negative_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END || '%)' as negative_sentiment_formatted,
            
            neutral_count || ' (' || 
            CASE 
                WHEN sentiment_total > 0 THEN ROUND((neutral_count * 100.0) / sentiment_total, 0)
                ELSE 0 
            END || '%)' as neutral_sentiment_formatted
        FROM firm_daily_analytics
        -- Optional: Filter out days with no sentiment data
        -- WHERE sentiment_total > 0
        ORDER BY 
            firm,
            date DESC;

        """

        top_influencers_by_mentions = """

           WITH influencer_analytics AS (
            SELECT 
            ud.username,
            tf.firm_id,
            fd.firm,
            dd.date,
            dd.day,
            dd.month,
            dd.year,
            -- Daily aggregations per influencer per firm
            SUM(tf.mentions) as daily_mentions,
            SUM(tf.no_views) as daily_reach,
            COUNT(tf.tweet_id) as daily_tweets
        FROM tweet_fact tf
        INNER JOIN user_dim ud ON tf.user_id = ud.user_id      -- Join on distribution key
        INNER JOIN firm_dim fd ON tf.firm_id = fd.firm_id      -- Join on distribution key  
        INNER JOIN date_dim dd ON tf.date_id = dd.date_id      -- Join on sort key
        WHERE tf.mentions IS NOT NULL
            -- Remove the mentions > 0 filter as it might be too restrictive
            -- Optional: Add date range filter
            -- AND dd.date >= '2024-01-01'
            -- AND dd.date <= '2024-12-31'
            -- Optional: Filter by specific firms
            -- AND fd.firm IN ('MTN', 'Airtel', 'Glo', '9mobile')
        GROUP BY 
            ud.username,
            tf.firm_id,
            fd.firm,
            dd.date,
            dd.day,
            dd.month,
            dd.year
    )
    SELECT 
        username,
        -- Format total mentions in readable format
        CASE 
            WHEN daily_mentions >= 1000000 THEN ROUND(daily_mentions/1000000.0, 1) || 'M'
            WHEN daily_mentions >= 1000 THEN ROUND(daily_mentions/1000.0, 1) || 'K'
            ELSE daily_mentions::VARCHAR
        END as total_mentions_formatted,
        daily_mentions as total_mentions,
        -- Format total reach in readable format
        CASE 
            WHEN daily_reach >= 1000000 THEN ROUND(daily_reach/1000000.0, 1) || 'M'
            WHEN daily_reach >= 1000 THEN ROUND(daily_reach/1000.0, 1) || 'K'
            ELSE daily_reach::VARCHAR
        END as total_reach_formatted,
        daily_reach as total_reach,
        date,
        day,
        month,
        year,
        firm,
        daily_tweets,
        -- Additional useful metrics
        ROUND(daily_mentions::DECIMAL / daily_tweets, 2) as avg_mentions_per_tweet,
        CASE 
            WHEN daily_reach > 0 THEN ROUND((daily_mentions * 100.0) / daily_reach, 2)
            ELSE 0 
        END as mention_rate_percent
    FROM influencer_analytics
    -- Sort by most mentions first
    ORDER BY 
        daily_mentions DESC,
        daily_reach DESC,
        username
                    """
        top_profiles = """

        WITH profile_daily_metrics AS (
            SELECT 
                ud.username,
                ud.followers_count,
                fd.firm,
                dd.date,
                dd.day,
                dd.month,
                dd.year,
                -- Daily aggregations per profile
                COUNT(tf.tweet_id) as daily_posts,
                SUM(tf.no_views) as daily_reach,
                SUM(tf.no_likes + tf.no_retweets + tf.no_replies + tf.no_quotes + tf.no_bookmarks) as daily_engagement,
                -- Calculate engagement rate
                CASE 
                    WHEN SUM(tf.no_views) > 0 THEN 
                        ROUND(((SUM(tf.no_likes + tf.no_retweets + tf.no_replies + tf.no_quotes + tf.no_bookmarks) * 100.0) / SUM(tf.no_views)), 0)
                    ELSE 0 
                END as engagement_rate
            FROM tweet_fact tf
            INNER JOIN user_dim ud ON tf.user_id = ud.user_id      -- Join on distribution key
            INNER JOIN firm_dim fd ON tf.firm_id = fd.firm_id      -- Join on distribution key  
            INNER JOIN date_dim dd ON tf.date_id = dd.date_id      -- Join on sort key
            WHERE ud.followers_count IS NOT NULL
                AND ud.followers_count > 0
                -- Optional: Add date range filter
                -- AND dd.date >= '2024-01-01'
                -- AND dd.date <= '2024-12-31'
                -- Optional: Filter by minimum followers
                -- AND ud.followers_count >= 100
            GROUP BY 
                ud.username,
                ud.followers_count,
                fd.firm,
                dd.date,
                dd.day,
                dd.month,
                dd.year
        )
        SELECT 
            username,
            firm,
            date,
            day,
            month,
            year,
            -- Format followers count in readable format
            CASE 
                WHEN followers_count >= 1000000 THEN ROUND(followers_count/1000000.0, 1) || 'M'
                WHEN followers_count >= 1000 THEN ROUND(followers_count/1000.0, 1) || 'K'
                ELSE followers_count::VARCHAR
            END as followers_formatted,
            followers_count as followers,
            engagement_rate as engagement,
            daily_posts as posts_count,
            -- Additional useful metrics
            daily_engagement as total_engagement,
            daily_reach,
            CASE 
                WHEN daily_posts > 0 THEN ROUND(daily_engagement::DECIMAL / daily_posts, 1)
                ELSE 0 
            END as avg_engagement_per_post
        FROM profile_daily_metrics
        -- Sort by top profiles (highest engagement rate, then followers, then posts)
        ORDER BY 
            engagement_rate DESC,
            followers_count DESC,
            daily_posts DESC,
            username
        """


        overview_kpis_df = pd.read_sql(overview_kpis, conn)
        influencers_by_mention_df = pd.read_sql(top_influencers_by_mentions, conn)
        top_profiles_df = pd.read_sql(top_profiles, conn)

        if not (overview_kpis_df.empty and influencers_by_mention_df.empty and top_profiles_df.empty):
            overview_kpis_result = overview_kpis_df.to_dict(orient='records')
            influencers_by_mention_result = influencers_by_mention_df.to_dict(orient='records')
            top_profiles_result = top_profiles_df.to_dict(orient='records')

            logger.info(f"Analysis home data retrieved successfully:")
            return {
                "status": "success",
                "message": "All Analysis data retrieved successfully",
                "body": {
                    "overview_kpis": overview_kpis_result,
                    "top_influencers_by_mentions": influencers_by_mention_result,
                    "top_profiles": top_profiles_result
                }
            }
        else:
            return {
                "status": "warning",
                "message": "No data found for the analysis home page",
                "body": {
                    "overview_kpis": [],
                    "top_influencers_by_mentions": [],
                    "top_profiles": []
                }
            }
    except Exception as e:
        if isinstance(e, HTTPException):
            logger.error(f"An error occured while retrieving overview KPIs: {str(e)}")
            raise HTTPException(
                status_code=e.status_code,
                detail={
                    "status": "error",
                    "message": "Failed to retrieve overview KPIs",
                    "body": str(e)
                }
            )
        else:
            raise e



# if __name__ == "__main__":
#     asyncio.run(analysis_home())