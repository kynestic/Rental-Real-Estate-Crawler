import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os

# ScraperAPI key
API_KEY = ''

# Đọc danh sách URL từ file CSV
data_url = pd.read_csv(r'D:\CODING\DATN\data\raw\urls.csv')
urls = data_url.iloc[:, 0].tolist()

# Tên file kết quả
output_file = r"D:\CODING\DATN\data\raw\page_texts.json"

# Duyệt từng URL
for i, url in enumerate(urls, start=1):
    print(f"Đang xử lý URL {i}: {url}")

    try:
        # Gửi yêu cầu ScraperAPI
        payload = {
            'api_key': API_KEY,
            'url': url,
            'render': 'true',
        }
        r = requests.get('https://api.scraperapi.com/', params=payload)
        
        if r.status_code != 200:
            print(f"❌ Không tải được trang (mã {r.status_code})")
            continue

        soup = BeautifulSoup(r.text, 'html.parser')

        # Trích xuất toàn bộ text trên trang
        page_text = soup.get_text(strip=True)

        # Trích xuất geo từ các script JSON-LD
        lat, long = None, None
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                json_data = json.loads(script.string)
                if isinstance(json_data, dict) and 'geo' in json_data:
                    geo = json_data['geo']
                    lat = geo.get('latitude')
                    long = geo.get('longitude')
                    break
            except (json.JSONDecodeError, TypeError):
                continue

        print(f"✅ {page_text[:100]} | Lat: {lat}, Long: {long}")
        print("-" * 60)

        data_entry = {
            "url": url,
            "text": page_text,
            "lat": lat,
            "long": long
        }
        

        # Ghi vào file JSON (dạng từng dòng một - JSON Lines)
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data_entry, ensure_ascii=False) + '\n')

    except Exception as e:
        print(f"❌ Lỗi khi xử lý URL {url}: {e}")
