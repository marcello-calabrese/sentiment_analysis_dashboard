import pandas as pd
import numpy as np
import psycopg2
from string import punctuation
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob     
import warnings

# ignore warnings
warnings.filterwarnings('ignore')
                                                                            


# import necessary custom libraries
from components.db_conn import CONN
from components.logger import logging 
from components.utils import insert_sentiment_data_to_pgdb


# establish connection to the cars_cleaned_sample table and read it into a dataframe

conn = CONN

sql_query = "SELECT * FROM cars_cleaned_sample"

# read the data into a dataframe

df = pd.read_sql_query(sql_query, conn)

# build the sentiment analysis pipeline and function

# Load Spacy model
nlp = spacy.load('en_core_web_lg')

# Add SpacyTextBlob to the pipeline
nlp.add_pipe("spacytextblob")

# Define function to perform sentiment analysis on text
def get_sentiment(text):
    doc = nlp(text)
    # get the sentiment score
    sentiment_score = doc._.blob.polarity
    # get the sentiment label
    if sentiment_score > 0.5:
        sentiment_label = "Positive"
    elif sentiment_score < 0:
        sentiment_label = "Negative"
    else:
        sentiment_label = "Neutral"
    return sentiment_score, sentiment_label

logging.info('Starting Sentiment Analysis')



# Apply function to Review column
df['sentiment_score'], df['sentiment_label'] = zip(*df['review'].apply(get_sentiment))

logging.info('Sentiment Analysis Completed')

# function to extract the keywords from the review column

def get_keywords(text):
    result = []
    pos_tag = ['PROPN', 'ADJ', 'NOUN'] 
    doc = nlp(text.lower()) # 
    for token in doc:
        # 3
        if(token.text in nlp.Defaults.stop_words or token.text in punctuation):
            continue
        # 4
        if(token.pos_ in pos_tag):
            result.append(token.text)
                
    return result # list of keywords

# apply the function to the review column

logging.info('Starting Keyword Extraction')

df['keywords'] = df['review'].apply(get_keywords)

logging.info('Keyword Extraction Completed')
# insert the sentiment dataframe into the database

logging.info('Inserting Sentiment Data into Database...')

insert_sentiment_data_to_pgdb(conn, df, table_name='cars_cleaned_sentiment')

logging.info('Sentiment Data Inserted into Database')

