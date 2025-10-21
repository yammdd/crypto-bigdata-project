import os
import google.generativeai as genai
from newsapi import NewsApiClient
from datetime import datetime, timedelta

# Cấu hình API clients từ biến môi trường
try:
    genai.configure(api_key=os.environ.get("GOOGLE_AI_API_KEY"))
    newsapi = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))
    model = genai.GenerativeModel("gemini-2.5-flash-lite")
except (TypeError, ValueError) as e:
    print(f"CẢNH BÁO: Không thể cấu hình API. Vui lòng kiểm tra biến môi trường GOOGLE_AI_API_KEY và NEWS_API_KEY. Lỗi: {e}")
    model = None
    newsapi = None

def get_latest_crypto_news(query="crypto OR bitcoin OR ethereum", days=1, page_size=5):
    """Lấy các bài báo mới nhất liên quan đến crypto."""
    if not newsapi:
        return ["Dịch vụ NewsAPI chưa được cấu hình."]
    try:
        from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        response = newsapi.get_everything(
            q=query,
            from_param=from_date,
            sort_by="relevancy",
            language="en",
            page_size=page_size
        )
        articles = [f"- {a['title']}: {a.get('description', 'N/A')}" for a in response.get("articles", [])]
        return articles
    except Exception as e:
        print(f"Lỗi khi lấy tin tức từ NewsAPI: {e}")
        return [f"Lỗi khi lấy tin tức: {e}"]

def get_market_analysis(news_summary, real_time_data_summary):
    """Sử dụng Gemini để phân tích tin tức và dữ liệu thời gian thực."""
    if not model:
        return "Mô hình Gemini LLM chưa được cấu hình."

    prompt = f"""
    As a cryptocurrency market analyst, your task is to provide a concise, insightful analysis based on the latest news and a snapshot of real-time market data.

    Here is a summary of the latest top news headlines:
    {news_summary}

    Here is a summary of real-time market data from the last few seconds:
    {real_time_data_summary}

    Based on all the information above, please provide a brief market analysis. Focus on the current market sentiment (e.g., bullish, bearish, uncertain) and potential short-term impacts on major cryptocurrencies like Bitcoin and Ethereum.
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Lỗi khi gọi Gemini API: {e}")
        return f"Lỗi phân tích từ Gemini: {e}"