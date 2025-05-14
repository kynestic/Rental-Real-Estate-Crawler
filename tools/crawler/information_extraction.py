import openai
import json
import pandas as pd

# Khởi tạo client với API key
client_auth = openai.OpenAI(api_key="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")

# Prompt đã cập nhật
prompt = """
Từ một đoạn văn bản mô tả bất động sản, hãy trích xuất các thông tin tương ứng với từng căn hộ hoặc đơn vị bất động sản riêng biệt (nếu có nhiều đơn vị trong cùng một văn bản).

Kết quả trả về dưới dạng một danh sách các đối tượng JSON.

Mỗi đối tượng tương ứng với một đơn vị bất động sản.

Nếu thông tin nào không được đề cập trong văn bản, hãy gán giá trị "unknown" cho trường đó.

Chỉ trích xuất đúng giá trị. Không thêm mô tả, diễn giải hay giải thích nào khác.

Không bao gồm bất kỳ chú thích, tiêu đề hay văn bản ngoài danh sách JSON.

📋 Các trường cần trích xuất cho mỗi đơn vị bất động sản:
Trường	Ý nghĩa
area_sqft	Diện tích (feet vuông)
price_usd	Giá tiền (USD)
studio	Có phải là studio không? (yes/no)
bedroom	Số phòng ngủ
bathroom	Số phòng tắm
walk_score	Điểm đi bộ (thường có thể có sau cụm từ "Walk Score®"dạng [điểm số]/100)
transit_score	Điểm phương tiện công cộng (thường có thể có sau cụm từ "Transit Score®"dạng [điểm số]/100)
bike_score	Điểm đi xe đạp (thường có thể có sau cụm từ "Bike Score®" dạng [điểm số]/100)
parking_lot	Có chỗ đậu xe không? (yes/no)
parking_covered	Có mái che không? (yes/no)
dryer	Có máy sấy không? (yes/no)
air_conditioner	Có điều hòa không? (yes/no)
washing_machine	Có máy giặt không? (yes/no)
heating Có hệ thống sưởi không (yes/no)
distance_to_park	Khoảng cách đến công viên gần nhất (ví dụ: "300m", "2km")
distance_to_hospital	Khoảng cách đến bệnh viện gần nhất (ví dụ: "500m", "1.5km")
distance_to_supermarket	Khoảng cách đến siêu thị gần nhất (ví dụ: "100m", "800m")
playground	Có sân chơi không? (yes/no)
lease_term_month	Thời hạn thuê (số tháng)
cat_allowed	Cho phép nuôi mèo không? (yes/no)
cat_fee	Có tính phí nuôi mèo không? (yes nếu có ghi chú cần đóng thêm phí nuôi mèo, no nếu không ghi yêu cầu đóng phí nuôi mèo)
dog_allowed	Cho phép nuôi chó không? (yes/no)
dog_fee	Có tính phí nuôi chó không? (yes nếu có ghi chú cần đóng thêm phí nuôi chó, no nếu không ghi yêu cầu đóng phí nuôi chó)

✅ Định dạng đầu ra mong muốn:

[
  {
    "area_sqft": "...",
    "price_usd": "...",
    "studio": "...",
    "bedroom": "...",
    "bathroom": "...",
    "walk_score": "...",
    "transit_score": "...",
    "bike_score": "...",
    "parking_lot": "...",
    "parking_covered": "...",
    "dryer": "...",
    "air_conditioner": "...",
    "washing_machine": "...",
    "distance_to_park": "...",
    "distance_to_hospital": "...",
    "distance_to_supermarket": "...",
    "playground": "...",
    "lease_term_month": "...",
    "cat_allowed": "...",
    "cat_fee": "...",
    "dog_allowed": "...",
    "dog_fee": "..."
  }
]

Nếu trong văn bản có nhiều đơn vị/căn hộ, hãy thêm nhiều đối tượng JSON tương ứng vào danh sách trên.

Văn bản cần được trích xuất dữ liệu như sau:"""


def get_json_type(client, real_estate_description):
  # Gửi yêu cầu tới GPT
  response = client.chat.completions.create(
      model="gpt-4o-mini",  # hoặc "gpt-3.5-turbo"
      messages=[
          {"role": "user", "content": prompt+real_estate_description}
      ],
      temperature=0
  )

  # Lấy nội dung từ phản hồi
  reply_text = response.choices[0].message.content.strip()

  # Chuyển đổi thành JSON
  try:
      result = json.loads(reply_text)
      print("✅ Dữ liệu trích xuất:")
      return result
  except json.JSONDecodeError:
      print("❌ Kết quả không phải JSON hợp lệ:")
      return [{"message": "error"}]


k = 0

file_exists = False

with open(r"D:\CODING\DATN\data\raw\page_texts.json", "r", encoding="utf-8") as f:
  
  for line in f:
        k += 1
        print(f"Processing line {k}")
        
        try:
            data = json.loads(line)

            # Gọi hàm để trích xuất mảng JSON từ đoạn text
            data_extracted = get_json_type(client_auth, data['text'])  # Trả về list[dict]

            # Thêm lon/lat vào từng phần tử trong mảng
            for item in data_extracted:
                item['lon'] = data.get('long')
                item['lat'] = data.get('lat')

            # Ghi vào CSV
            df = pd.DataFrame(data_extracted)
            df.to_csv(r'D:\CODING\DATN\data\raw\tmp.csv', mode='a', index=False, header=not file_exists)
            file_exists = True

        except Exception as e:
            print(f"Error on line {k}: {e}")

        if k > 10:
            break

