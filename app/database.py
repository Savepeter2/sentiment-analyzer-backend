import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from fastapi import HTTPException, status
from configs.config import (
    USER_DIM_TABLE, TOPIC_DIM_TABLE, FIRM_DIM_TABLE,
    FACT_TWEETS_TABLE, DATE_DIM_TABLE,
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE,
    REDSHIFT_PASSWORD, REDSHIFT_USERNAME,
    ACCESS_KEY_ID,
    SECRET_ACCESS_KEY
)

import redshift_connector


def connect_to_redshift(redshift_config: dict):
    """
    Create a connection to the Redshift database using the provided configuration.
    Args:
        redshift_config: Dictionary containing Redshift connection parameters.
    Returns:
        engine: SQLAlchemy engine connected to Redshift.
    """
    try:

        conn = redshift_connector.connect(
     host=REDSHIFT_HOST,
     port=REDSHIFT_PORT,
     user=REDSHIFT_USERNAME,
     password=REDSHIFT_PASSWORD,
     access_key_id=ACCESS_KEY_ID,
     secret_access_key=SECRET_ACCESS_KEY,
        database=REDSHIFT_DATABASE

  )     
        return conn

        # return engine

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status": "error",
                "message": "Failed to connect to Redshift",
                "body": str(e)
            }
        )

# run the function to connect to Redshift
conn = connect_to_redshift({
    'user': REDSHIFT_USERNAME,
    'password': REDSHIFT_PASSWORD,
    'host': REDSHIFT_HOST,
    'port': REDSHIFT_PORT,
    'database': REDSHIFT_DATABASE,
    'access_key_id': ACCESS_KEY_ID,
    'secret_access_key': SECRET_ACCESS_KEY
})

