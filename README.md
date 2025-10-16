clone về xong vào thư mục

```bash
cd crypto-bigdata-project
```

xong, tạo 1 thư mục `crypto-bigdata-project/models` để ko bị lỗi
```bash
docker compose up -d --build
```

tại thư mục `crypto-bigdata-project`

chạy
```bash
docker exec -it spark-master bash
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

rồi lại (phần batch, nếu mà sửa UI thì ko cần chạy cũng dc)
```bash
spark-submit /opt/spark/work-dir/batch/write_to_mongo.py
```

**Lưu ý:** Chỉ chạy các lệnh này 1 lần duy nhất, các lần sau chỉ cần

```bash
docker compose up -d --build
```

vào frontend trên 
`localhost:5000`

batch layer trên
`localhost:8501` (cái này vẫn còn lỗi, tính sau)

hi vọng là chạy được trên máy ae hihihi

có thể sửa các loại coin ae thích, cần thay đổi tên ở tất cả các file liên quan (**rất quan trọng**) rồi chạy lại theo từng bước, dm có khi phải đổi btc sang cái khác chứ nó làm xấu chart của t