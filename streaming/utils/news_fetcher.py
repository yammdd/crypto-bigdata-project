import os
from newsapi import NewsApiClient
from datetime import datetime, timedelta

try:
    newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))
except Exception as e:
    print(f"CẢNH BÁO: Không thể cấu hình NewsAPI. Lỗi: {e}")
    newsapi = None

def get_latest_crypto_news(query="crypto OR bitcoin OR ethereum", days=1, page_size=5):
    if not newsapi:
        return ["News service is not configured."]
    try:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        response = newsapi.get_everything(
            q=query, from_param=from_date, sort_by="relevancy", language="en", page_size=page_size
        )
        articles = [f"- {a['title']}: {a.get('description', 'N/A')}" for a in response.get("articles", [])]
        return articles
    except Exception as e:
        return [f"Error fetching news: {e}"]