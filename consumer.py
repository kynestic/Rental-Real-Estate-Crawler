from kafka import KafkaConsumer
import json

# Khởi tạo consumer
consumer1 = KafkaConsumer(
    'redfin_rental',  # Tên topic
    bootstrap_servers=['localhost:19092'],  # Địa chỉ Kafka broker
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Giải mã JSON
)

consumer2 = KafkaConsumer(
    'realtor_rental',  # Tên topic
    bootstrap_servers=['localhost:19092'],  # Địa chỉ Kafka broker
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Giải mã JSON
)

consumer3 = KafkaConsumer(
    'apartment_rental',  # Tên topic
    bootstrap_servers=['localhost:19092'],  # Địa chỉ Kafka broker
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Giải mã JSON
)

print("✅ Đang lắng nghe dữ liệu từ Kafka topic: redfin_rental...\n")

for message in consumer1:
    print("📦 Dữ liệu nhận được:")
    print(json.dumps(message.value, indent=2))  # In đẹp dữ liệu
