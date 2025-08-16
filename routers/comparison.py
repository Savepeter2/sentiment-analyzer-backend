import os
import asyncio
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import APIRouter, HTTPException
from configs.config import (
    logger
)
from app.database import conn
import pandas as pd

router = APIRouter()


@router.get("/comparison", tags=["Comparison"])
async def comparison(
) -> dict:
    
    """
    Endpoint to retrieve Comparison KPIS data for the comparison page.

    This Endpoint returns all required fields and their respective values for the comparison page.

    Args:
        None

    Returns:

        dict: A dictionary containing the status, message, and body with the following structure:

        - comparison_kpis: List of dictionaries with the following keys:
            - firm: Name of the firm
            - firm_id: ID of the firm
            - date: Date of the data
            - day: Day of the month
            - month: Month of the year
            - year: Year of the data
            - total_mentions_formatted: Formatted total mentions (e.g., "1K", "2M")
            - total_mentions: Total number of mentions
            - total_reach_formatted: Formatted total reach (e.g., "1K", "2M")
            - total_reach: Total reach value
            - positive_count: Count of positive mentions
            - positive_percent: Percentage of positive mentions
            - negative_count: Count of negative mentions
            - negative_percent: Percentage of negative mentions
            - neutral_count: Count of neutral mentions
            - neutral_percent: Percentage of neutral mentions
            - sentiment_total: Total number of mentions with valid sentiment
            - positive_sentiment_formatted: Formatted positive sentiment with percentage (e.g., "500 (50%)")
            - negative_sentiment_formatted: Formatted negative sentiment with percentage (e.g., "300 (30%)")
            - neutral_sentiment_formatted: Formatted neutral sentiment with percentage (e.g., "200 (20%)")

    Example:
    ```json
    {
        "status": "success",
        "message": "Comparison home data retrieved successfully",
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
            }]
        }
    }
    ```
        }
"""
    try:
        comparison_kpis = """

        WITH firm_daily_analytics AS (
            SELECT 
                tf.firm_id,
                fd.firm,
                dd.date,
                dd.day,
                dd.month,
                dd.year,
                -- Total metrics
                -- SUM(tf.mentions) as total_mentions,
                SUM(tf.no_views) as total_reach,
                COUNT(*) as total_mentions,
                -- Sentiment counts
                SUM(CASE WHEN tf.consensus_sentiment = 'Positive' THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN tf.consensus_sentiment = 'Negative' THEN 1 ELSE 0 END) as negative_count,
                SUM(CASE WHEN tf.consensus_sentiment = 'Neutral' THEN 1 ELSE 0 END) as neutral_count,
                -- Count tweets with valid sentiment
                SUM(CASE WHEN tf.consensus_sentiment IN ('Positive', 'Negative', 'Neutral') THEN 1 ELSE 0 END) as sentiment_total
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

        comparison_kpis_df = pd.read_sql(comparison_kpis, conn)

        if not comparison_kpis_df.empty:
            comparison_kpis_result = comparison_kpis_df.to_dict(orient='records')

            logger.info(f"Comparison data retrieved successfully:")
            return {
                "status": "success",
                "message": "All Comparison data retrieved successfully",
                "body": {
                    "comparison_kpis": comparison_kpis_result
                }
            }
        else:
            return {
                "status": "warning",
                "message": "No data found for the comparison page",
                "body": {
                    "comparison_kpis": []
                }
            }
    except Exception as e:
        if isinstance(e, HTTPException):
            logger.error(f"An error occured while retrieving comparison KPIs: {str(e)}")
            raise HTTPException(
                status_code=e.status_code,
                detail={
                    "status": "error",
                    "message": "Failed to retrieve comparison KPIs",
                    "body": str(e)
                }
            )
        else:
            raise e
