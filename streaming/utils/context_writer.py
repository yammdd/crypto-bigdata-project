from pymongo import MongoClient

CONTEXT_DOCUMENT_ID = "latest_market_context"

def update_market_context(news_summary, real_time_data_list):
    """Lưu bối cảnh thị trường mới nhất (dữ liệu chi tiết) vào MongoDB."""
    try:
        client = MongoClient('mongodb', 27017)
        db = client['crypto_db']
        collection = db['market_context']

        context_document = {
            "news_summary": news_summary,
            "real_time_data": real_time_data_list
        }
        
        collection.update_one(
            {"_id": CONTEXT_DOCUMENT_ID},
            {"$set": context_document},
            upsert=True
        )
        print("Đã cập nhật bối cảnh thị trường chi tiết vào MongoDB.")
    except Exception as e:
        print(f"Lỗi khi cập nhật bối cảnh vào MongoDB: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()