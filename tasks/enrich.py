import logging
from sqlalchemy import select
from utils.db_util import Session, News, engine
from utils.enrich_util import enrich
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clean_news():
    logging.info("Starting to retrieve clean news items from database")
    session = Session()
    try:
        query = select(News).where(News.status == 'clean')
        result = session.execute(query)
        news_items = result.scalars().all()
        logging.info(f"Retrieved {len(news_items)} clean news items")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([
        {
            'id': item.id,
            'title': item.title,
            'link': item.link,
            'company': item.company,
            'published_date': item.published_date,
            'publisher_topic': item.publisher_topic,
            'publisher': item.publisher,
            'status': item.status
        } for item in news_items
    ])
    logging.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def update_enriched_news(enriched_df):
    logging.info("Starting to update database with enriched news data")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            if index % 10 == 0:  # Log progress every 10 items
                logging.info(f"Updating item {index + 1} of {total_items}")
            news_item = session.query(News).get(row['id'])
            if news_item:
                news_item.content = row['content']
                news_item.ai_topic = row['ai_topic']
                news_item.ai_summary = row['ai_summary']
                news_item.status = 'enriched'
                updated_count += 1
        session.commit()
        logging.info(f"Updated {updated_count} news items with enriched data")
    except Exception as e:
        logging.error(f"Error updating enriched news: {e}")
        session.rollback()
    finally:
        session.close()

def main():
    start_time = time.time()
    logging.info("Starting news enrichment task")
    
    # Get clean news items
    clean_news = get_clean_news()
    
    if not clean_news:
        logging.info("No clean news items to enrich. Task completed.")
        return
    
    # Convert to DataFrame
    news_df = news_to_dataframe(clean_news)
    
    # Enrich the news
    logging.info("Starting news enrichment process")
    enriched_df = enrich(news_df)
    logging.info(f"Enrichment completed. Enriched DataFrame has {len(enriched_df)} rows and {len(enriched_df.columns)} columns")
    
    # Update the database with enriched data
    update_enriched_news(enriched_df)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"News enrichment task completed. Total duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
