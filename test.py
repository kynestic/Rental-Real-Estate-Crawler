from tools.crawler.redfin.Redfin import RedfinCrawler
from tools.crawler.apartment.Apartment import ApartmentCrawler
from tools.crawler.realtor.Realtor import RealtorCrawler
from data.raw.zipcode import zipcode
import json
import time
import os
import numpy as np

def deep_throtling():
    time.sleep(np.random.randint(300, 600))
# Hàm ghi log vào file
def log_to_file(message, log_file=r'D:\CODING\DATN\data\logs\tmp.log'):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(message + '\n')

def main_func():
    k = 0
    for item in zipcode:
        k += 1
        log_to_file(f'zipcode number {k}/{len(zipcode)}')

        try:
            apartment = ApartmentCrawler(str(item))
            realtor = RealtorCrawler(str(item))
            redfin = RedfinCrawler(str(item))
        except Exception as e:
            log_to_file(f'Error initializing crawlers: {e}')

        try:
            log_to_file('get realtor data')
            start_time = time.time()
            with open('realtor.jsonl', 'a', encoding='utf-8') as f:
                final_data = realtor.getData()
                for item in final_data:
                    item = str(item)
                    item = item.replace('\n','')
                    f.write(item+'\n')
            end_time = time.time()
            log_to_file(f'Finished Realtor in {end_time - start_time:.2f} seconds')
        except Exception as e:
            log_to_file(f'Error Realtor: {e}')

        continue

        
        

        try:
            log_to_file('get Apartment data')
            start_time = time.time()
            os.makedirs('data/raw', exist_ok=True)
            with open('data/raw/output_apartment.jsonl', 'a', encoding='utf-8') as f:
                final_data = apartment.getData()
                json.dump(final_data, f, ensure_ascii=False, indent=4)
                f.write('\n')
            end_time = time.time()
            log_to_file(f'Finished Apartment in {end_time - start_time:.2f} seconds')
        except Exception as e:
            log_to_file(f'Error Apartment: {e}')

        continue
        # Save Redfin data
        try:
            log_to_file('get redfin data')
            start_time = time.time()
            os.makedirs('data/raw', exist_ok=True)
            with open('data/raw/output_redfin.jsonl', 'a', encoding='utf-8') as f:
                final_data = redfin.getData()
                json.dump(final_data, f, ensure_ascii=False, indent=4)
                f.write('\n')
            end_time = time.time()
            log_to_file(f'Finished Redfin in {end_time - start_time:.2f} seconds')
        except Exception as e:
            log_to_file(f'Error Redfin: {e}')

        # Save Realtor data
        
main_func()


# print(data)
# print("Đã thay thế thành công và lưu vào file mới!")
# data = []
# with open('data_converted.jsonl', 'r') as out_file:
    
#     for line in out_file:
#         # Bỏ qua dòng trống
#         if line.strip():  # line.strip() sẽ trả về True nếu dòng không phải là trắng
#             try:
#                 data.append(json.loads(line))
#             except json.JSONDecodeError as e:
#                 print(f"Error decoding line: {line} - {e}")
# with open('data_converted.jsonl', 'w') as outfile:
#     for entry in data:
#         # Chuyển đối tượng Python thành chuỗi JSON và ghi vào tệp
#         json.dump(entry, outfile)
#         # Thêm một dòng mới sau mỗi đối tượng JSON để tạo thành định dạng JSONL
#         outfile.write('\n')

# print("Dữ liệu đã được ghi vào tệp output.jsonl")

# import requests
# headers = {
#             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#             'accept-language': 'en-US,en;q=0.9',
#             'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="122", "Chromium";v="122"',
#             'sec-ch-ua-mobile': '?1',
#             'sec-ch-ua-platform': '"Android"',
#             'sec-fetch-dest': 'iframe',
#             'sec-fetch-mode': 'navigate',
#             'sec-fetch-site': 'cross-site',
#             'sec-fetch-user': '?1',
#             'upgrade-insecure-requests': '1',
#             'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
#             'referer': 'www.apartments.com',
#             'origin':'https://www.apartments.com',
#         }
# url = 'https://www.apartments.com/broadstone-inkwell-long-beach-ca/eqg21kp/'
