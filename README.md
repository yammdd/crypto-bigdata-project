bây giờ nhé, clone về máy

```bash
cd crypto-bigdata-project
```

xong 
```bash
docker compose up -d --build
```

thế là xong, chạy ngon

làm đéo gì có chuyện ngon thế

tại thư mục `crypto-bigdata-project`

chạy
```bash
docker compose exec spark-master bash
```

xong 
```bash
export PATH=$PATH:/opt/spark/bin
```

rồi 
```bash
spark-submit /opt/spark/work-dir/batch/yahoo_to_hdfs.py
```

đợi 1 lúc cho nó chạy xong, rồi chạy tiếp cái này

```bash
spark-submit /opt/spark/work-dir/batch/train_all_xgboost_models.py
```

**Lưu ý:** Chỉ chạy các lệnh này 1 lần duy nhất, các lần sau chỉ cần

```bash
docker compose up -d --build
```

vào frontend trên 
`localhost:5000`

hi vọng là chạy được trên máy ae hihihi