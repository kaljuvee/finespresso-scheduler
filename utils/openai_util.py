import os
from openai import OpenAI
from dotenv import load_dotenv
from gptcache import cache

load_dotenv()

client = OpenAI()
cache.init()
cache.set_openai_key()

# Load environment variables

model_name = "gpt-4o-mini"  # Updated model name

def tag_news(news, tags):
    prompt = f'Answering with one tag only, pick up the best tag which describes the news "{news}" from the list: {tags}'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    tag = response.choices[0].message.content
    return tag

def summarize(news):
    prompt = f'Summarize this in a brief, exciting way like a sports commentary (50 words or less): "{news}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    summary = response.choices[0].message.content
    return summary