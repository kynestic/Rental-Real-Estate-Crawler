from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType
import json

# Khởi tạo SparkSession
spark = SparkSession.builder.master("local[*]").appName("KafkaDataProcessing").getOrCreate()

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("listingid", StringType(), True),  # ID bài đăng
    StructField("source", StringType(), True),  # Nguồn
    StructField("price", FloatType(), True),  # Giá tiền
    StructField("beds", FloatType(), True),  # Số phòng ngủ
    StructField("sqft", FloatType(), True),  # Diện tích
    StructField("baths", FloatType(), True),  # Số phòng tắm
    StructField("lat", FloatType(), True),  # Vĩ độ
    StructField("long", FloatType(), True),  # Kinh độ
    StructField("url", StringType(), True),  # URL
    StructField("addy", StringType(), True),  # Địa chỉ
    StructField("current_time", StringType(), True),  # Thời gian crawl
    StructField("has-pool", BooleanType(), True),  # Có bể bơi không
    StructField("dogs-allowed", BooleanType(), True),  # Cho phép chó không
    StructField("cats-allowed", BooleanType(), True),  # Cho phép mèo không
    StructField("air-conditioning", BooleanType(), True),  # Có điều hòa không
    StructField("washer-dryer-in-unit", BooleanType(), True),  # Máy giặt trong đơn vị
    StructField("has-laundry-facility", BooleanType(), True),  # Có tiện ích giặt là
    StructField("has-laundry-hookups", BooleanType(), True),  # Có kết nối giặt là
    StructField("has-dishwasher", BooleanType(), True),  # Có máy rửa chén không
    StructField("has-parking", BooleanType(), True),  # Có chỗ đậu xe không
    StructField("utilities-included", BooleanType(), True),  # Bao gồm tiện ích không
    StructField("is-furnished", BooleanType(), True),  # Có nội thất không
    StructField("has-short-term-lease", BooleanType(), True),  # Cho thuê ngắn hạn không
    StructField("is-senior-living", BooleanType(), True),  # Dành cho người cao tuổi không
    StructField("is-income-restricted", BooleanType(), True)  # Giới hạn thu nhập không
])

# Kafka consumer
consumer = KafkaConsumer(
    'redfin_rental',  # Tên topic
    bootstrap_servers=['localhost:19092'],  # Địa chỉ Kafka broker
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Giải mã JSON
)

# Đọc dữ liệu từ Kafka và chuyển thành DataFrame
for message in consumer:
    # Dữ liệu từ Kafka dưới dạng JSON
    data = message.value
    
    # Chuyển đổi dữ liệu thành DataFrame
    df = spark.createDataFrame([data], schema)
    
    # Hiển thị dữ liệu nhận được
    df.show(truncate=False)
    
    # Ví dụ xử lý dữ liệu: Chuyển đổi thời gian từ chuỗi thành timestamp
    from pyspark.sql.functions import col
    df = df.withColumn("current_time", 
                       col("current_time").cast("timestamp"))
    
    # Hiển thị lại DataFrame sau khi chuyển đổi
    df.show(truncate=False)
    
    # Bạn có thể xử lý hoặc lưu trữ dữ liệu sau khi xử lý, ví dụ:
    # df.write.format("parquet").save("processed_data.parquet")
    
# Dừng SparkSession khi hoàn thành
spark.stop()
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType
import json

# Khởi tạo SparkSession
spark = SparkSession.builder.master("local[*]").appName("KafkaDataProcessing").getOrCreate()

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("listingid", StringType(), True),  # ID bài đăng
    StructField("source", StringType(), True),  # Nguồn
    StructField("price", FloatType(), True),  # Giá tiền
    StructField("beds", FloatType(), True),  # Số phòng ngủ
    StructField("sqft", FloatType(), True),  # Diện tích
    StructField("baths", FloatType(), True),  # Số phòng tắm
    StructField("lat", FloatType(), True),  # Vĩ độ
    StructField("long", FloatType(), True),  # Kinh độ
    StructField("url", StringType(), True),  # URL
    StructField("addy", StringType(), True),  # Địa chỉ
    StructField("current_time", StringType(), True),  # Thời gian crawl
    StructField("has-pool", BooleanType(), True),  # Có bể bơi không
    StructField("dogs-allowed", BooleanType(), True),  # Cho phép chó không
    StructField("cats-allowed", BooleanType(), True),  # Cho phép mèo không
    StructField("air-conditioning", BooleanType(), True),  # Có điều hòa không
    StructField("washer-dryer-in-unit", BooleanType(), True),  # Máy giặt trong đơn vị
    StructField("has-laundry-facility", BooleanType(), True),  # Có tiện ích giặt là
    StructField("has-laundry-hookups", BooleanType(), True),  # Có kết nối giặt là
    StructField("has-dishwasher", BooleanType(), True),  # Có máy rửa chén không
    StructField("has-parking", BooleanType(), True),  # Có chỗ đậu xe không
    StructField("utilities-included", BooleanType(), True),  # Bao gồm tiện ích không
    StructField("is-furnished", BooleanType(), True),  # Có nội thất không
    StructField("has-short-term-lease", BooleanType(), True),  # Cho thuê ngắn hạn không
    StructField("is-senior-living", BooleanType(), True),  # Dành cho người cao tuổi không
    StructField("is-income-restricted", BooleanType(), True)  # Giới hạn thu nhập không
])

# Kafka consumer
consumer = KafkaConsumer(
    'redfin_rental',  # Tên topic
    bootstrap_servers=['localhost:19092'],  # Địa chỉ Kafka broker
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Giải mã JSON
)

# Đọc dữ liệu từ Kafka và chuyển thành DataFrame
for message in consumer:
    # Dữ liệu từ Kafka dưới dạng JSON
    data = message.value
    
    # Chuyển đổi dữ liệu thành DataFrame
    df = spark.createDataFrame([data], schema)
    
    # Hiển thị dữ liệu nhận được
    df.show(truncate=False)
    
    # Ví dụ xử lý dữ liệu: Chuyển đổi thời gian từ chuỗi thành timestamp
    from pyspark.sql.functions import col
    df = df.withColumn("current_time", 
                       col("current_time").cast("timestamp"))
    
    # Hiển thị lại DataFrame sau khi chuyển đổi
    df.show(truncate=False)
    
    # Bạn có thể xử lý hoặc lưu trữ dữ liệu sau khi xử lý, ví dụ:
    # df.write.format("parquet").save("processed_data.parquet")
    
# Dừng SparkSession khi hoàn thành
spark.stop()
