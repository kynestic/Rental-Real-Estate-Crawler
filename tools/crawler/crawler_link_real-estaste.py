import requests
import re
import pandas as pd
import os
import time

# Function to fetch and process the page with retry
def fetch_page(page_num, max_retries=8, delay=3):
    url = f"https://www.realtor.com/apartments/90011/pg-{page_num}"
    payload = { 
        'api_key': '787aca77cfbd06ef9f45c8ec19626a31', 
        'url': url, 
        'wait_for_selector': 'ul li'
    }

    retries = 0
    while retries < max_retries:
        try:
            r = requests.get('https://api.scraperapi.com/', params=payload)
            if r.status_code == 200:
                # Ghi nội dung vào file tmp.html
                with open('tmp.html', 'w', encoding='utf-8') as file:
                    file.write(r.text)
                print(f"✅ Đã ghi HTML của trang {page_num} vào tmp.html.")
                return True
            elif r.status_code == 500:
                retries += 1
                print(f"⚠️ Lỗi 500 tại trang {page_num}. Thử lại ({retries}/{max_retries}) sau {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ Request thất bại cho trang {page_num}, mã lỗi: {r.status_code}")
                return False
        except Exception as e:
            print(f"‼️ Lỗi khi request trang {page_num}: {e}")
            return False

    print(f"❌ Quá số lần thử lại cho trang {page_num}. Bỏ qua.")
    return False

# Function to extract URLs and ensure they start with 'https://www.zillow.com/'
def extract_urls():
    with open('tmp.html', 'r', encoding='utf-8') as file:
        content = file.read()

    # Sử dụng regex để tìm các URL với "detailUrl"
    urls = re.findall(r'"detailUrl":"(.*?)"', content)

    if urls:
        url_list = []
        print("🔗 Các URL tìm được:")
        for url in urls:
            if not url.startswith("https://www.zillow.com/"):
                url = "https://www.zillow.com" + url
            url_list.append(url)
            print(url)

        # Ghi vào CSV
        df = pd.DataFrame(url_list, columns=["URL"])
        if os.path.exists('urls.csv'):
            df.to_csv("urls.csv", mode='a', header=False, index=False)
        else:
            df.to_csv("urls.csv", mode='w', header=True, index=False)
        
        print("📥 Đã thêm các URL vào file urls.csv.")
    else:
        print("⚠️ Không tìm thấy URL nào.")

# Duyệt qua các trang
for page_num in range(1, 21):
    if fetch_page(page_num):
        extract_urls()
