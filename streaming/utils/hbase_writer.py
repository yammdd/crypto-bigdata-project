import happybase
import os

# Hàm ghi một batch DataFrame vào HBase
def write_to_hbase(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Kết nối HBase
    connection = happybase.Connection(host=os.getenv("HBASE_THRIFT_HOST", "hbase"), port=9090)
    table = connection.table("crypto_prices")

    # Với mỗi hàng trong DataFrame
    for row in batch_df.collect():
        row_key = f"{row['symbol']}_{row['timestamp']}"
        table.put(row_key, {
            b'data:price': str(row['price']).encode('utf-8'),
            b'data:price_change': str(row['price_change']).encode('utf-8'),
            b'data:price_change_pct': str(row['price_change_pct']).encode('utf-8'),
            b'data:high_price': str(row['high_price']).encode('utf-8'),
            b'data:low_price': str(row['low_price']).encode('utf-8'),
            b'data:open_price': str(row['open_price']).encode('utf-8'),
            b'data:volume_token': str(row['volume_token']).encode('utf-8'),
            b'data:volume_quote': str(row['volume_quote']).encode('utf-8'),
            b'data:best_bid_price': str(row['best_bid_price']).encode('utf-8'),
            b'data:best_ask_price': str(row['best_ask_price']).encode('utf-8'),
            b'meta:source': row['source'].encode('utf-8'),
            b'meta:category': row['category'].encode('utf-8'),
            b'meta:symbol': row['symbol'].encode('utf-8'),
            b'meta:timestamp': str(row['timestamp']).encode('utf-8'),
        })

    connection.close()
