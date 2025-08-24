import logging
import os
import sys
import configparser
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from app.database import conn
import openai
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from configs.config import OPEN_AI_API_KEY, logger
# OpenAI API Ke
openai.api_key = OPEN_AI_API_KEY


def get_recommend_df_redshift(user_query: str) -> pd.DataFrame:

    telecom_keywords = ["telecom", "network provider", "service provider"]
    bank_keywords = ["bank", "banking"]

    if any(keyword in user_query.lower() for keyword in telecom_keywords):
        industry = "telecom"
    elif any(keyword in user_query.lower() for keyword in bank_keywords):
        industry = "bank"
    else:
        industry = "unknown"

    # print(f"Fetching recommendation data for industry: {industry}")

    if industry == "telecom":
        firms = ['mtn', 'airtel']
    elif industry == "bank":
        firms = ['gtbank', 'zenith']
    else:
        firms = []
    
    if firms:
        # Create placeholders for each firm
        placeholders = ','.join(['%s'] * len(firms))
        recommend_df_query = f"""
        SELECT fd.firm, tf.tweet_text, td.topic_name AS gpt3_topic, tf.gpt3_sentiment AS gpt3_sentiment
        FROM tweet_fact tf
        JOIN firm_dim fd ON tf.firm_id = fd.firm_id
        JOIN topic_dim td ON tf.topic_id = td.topic_id
        WHERE tf.gpt3_sentiment IS NOT NULL
        AND fd.firm IN ({placeholders})
        """
        params = tuple(firms)  

        df = pd.read_sql(recommend_df_query, conn, params=params)

        return df
    
    else:
        logger.info("No relevant data found in database for the industry provided in query, returning None")
        return None
      

def encode_topics(df: pd.DataFrame, model: SentenceTransformer):
    if not df.empty:
        # Encode topics for semantic search
        topics = df['gpt3_topic'].unique().tolist()
        topic_embeddings = model.encode(topics, convert_to_tensor=True)

        return {
            "status": "success",
            "message": "Topic embeddings and encoding generated successfully.",
            "topic_embeddings": topic_embeddings,
            "topics": topics,
            "model": model
        }
    else:
        logger.info("No relevant data found in database for the industry provided in query, returning None")
        return None


def analyze_query(user_query: str, df:pd.DataFrame, model: SentenceTransformer):

        if not df.empty and user_query:

            encoding_result = encode_topics(df, model)

            topic_embeddings = encoding_result["topic_embeddings"]
            topics = encoding_result["topics"]

            query_embedding = model.encode(user_query, convert_to_tensor=True)
            scores = util.cos_sim(query_embedding, topic_embeddings)[0]
            best_topic = topics[scores.argmax().item()]
            
            # Step 2: Filter dataset by topic
            subset = df[df['gpt3_topic'] == best_topic]

            # Step 3: Aggregate sentiment
            sentiment_scores = subset.groupby(['firm', 'gpt3_sentiment']).size().unstack(fill_value=0)

            # Ensure all firms are represented (even if missing in this topic)
            all_firms = df['firm'].unique()
            sentiment_scores = sentiment_scores.reindex(all_firms, fill_value=0)

            # Ensure sentiment columns exist
            for col in ["Positive", "Negative", "Neutral"]:
                if col not in sentiment_scores:
                    sentiment_scores[col] = 0

            # Step 4: Compute net sentiment score
            sentiment_scores["net_score"] = (
                (sentiment_scores["Positive"] - sentiment_scores["Negative"]) /
                sentiment_scores.sum(axis=1).replace(0, 1)   # avoid division by zero
            )

            # Sort firms by score
            ranked_scores = sentiment_scores.sort_values("net_score", ascending=False)

            return best_topic, ranked_scores

        else:
            logger.info("user_query or df or model is None")
            return None, None, None
    

def generate_chatbot_response(user_query:str):
    
    df = get_recommend_df_redshift(user_query)

    model = SentenceTransformer('all-MiniLM-L6-v2')

    best_topic, ranked_scores = analyze_query(user_query, df, model)

    # Build context for GPT
    stats_text = ranked_scores.to_string()

    prompt = f"""
    A user asked: "{user_query}"
    Based on past customer feedback, the most relevant topic is: {best_topic}.
    Here are the sentiment scores per firm:
    {stats_text}

    Recommend the best firm using the net sentiment score (Positive - Negative / Total).
    If no firm is clearly better, explain the trade-offs. 
    Write the answer in a friendly, conversational chatbot style.
    """
    
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",  
        messages=[{"role": "system", "content": "You are a helpful brand recommender chatbot."},
                  {"role": "user", "content": prompt}]
    )
    
    chatbot_response = response["choices"][0]["message"]["content"]

    return chatbot_response


# print(generate_chatbot_response("Which network provider is better in terms of low data consumption?"))
# # print(generate_chatbot_response("Which bank should I use for fewer service issues?"))
