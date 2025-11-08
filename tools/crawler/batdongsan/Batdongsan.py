# import các thư viện cần thiết
from bs4 import BeautifulSoup
from curl_cffi import requests
import re
import time

output_file = open(r'D:\CODING\Rental-Real-Estate-Crawler\tools\crawler\batdonsanlink.txt', 'a', encoding='utf-8')

tries = 0
response = None

# response = requests.get('https://batdongsan.com.vn/nha-dat-cho-thue', impersonate = 'chrome')
# soup = BeautifulSoup(response.text, 'html.parser')
# page_count = None
# all_page = soup.find_all('a', 're__pagination-number')
# print(all_page)
# page_count = int(re.sub(r'[^0-9]', '', all_page[-1].text))

# if not isinstance(page_count, int):
#     property_count = soup.find('span', id='count-number')
#     property_count = int(re.sub(r'[^0-9]', '', property_count.text))

#     property_per_page = soup.find_all('div', class_='js__card')
#     property_per_page = len(property_per_page)
#     print(property_per_page)
#     page_count = int(property_count/property_per_page) + 1

# start_time = time.time()
# print(page_count)
# for page in range(1, page_count +1 ):
#     try:
#         print('---------------------------------------------------------------------')
#         print(f"https://batdongsan.com.vn/nha-dat-cho-thue/p{page}")
#         response = requests.get(f"https://batdongsan.com.vn/nha-dat-cho-thue/p{page}", impersonate='chrome')
#         soup = BeautifulSoup(response.text, 'html.parser')
#         links = soup.find_all('a', class_="js__product-link-for-product-id")
#         print(response.status_code)
#         for link in links:
#             href = str(link.get('href'))
#             if href.startswith('/'):
#                 href = 'https://batdongsan.com.vn' + href
#                 output_file.write(href + '\n')
#             else:
#                 output_file.write(href + '\n')
#         if (page % 100 == 0):
#             end_time = time.time()
#             print('Thời gian xử lý: ', end_time - start_time)
#             start_time = time.time()
#     except NameError:
#         print("Có lỗi xảy ra!", NameError)

class BatdongsanCrawler():
    def __init__(self) -> None:
        """
        This crawler will only get the html from the web, 
        the transformation will happen later after being loaded into the object storage
        """
        pass

    def setPageCount(self) -> None:
        """
        set number of page that we will crawl from
        """
        response = requests.get('https://batdongsan.com.vn/nha-dat-cho-thue', impersonate = 'chrome')
        soup = BeautifulSoup(response.text, 'html.parser')

        page_count = None
        all_page = soup.find_all('a', 're__pagination-number')
        print(all_page)
        page_count = int(re.sub(r'[^0-9]', '', all_page[-1].text))

        if not isinstance(page_count, int):
            property_count = soup.find('span', id='count-number')
            property_count = int(re.sub(r'[^0-9]', '', property_count.text))

            property_per_page = soup.find_all('div', class_='js__card')
            property_per_page = len(property_per_page)
            print(property_per_page)
            page_count = int(property_count/property_per_page) + 1
        
        self.page_count = page_count

    def getPropertyDetail(self) -> None:
        """
        In this function, we will crawl each page (and store its snapshot) from the search list the get the url list
        With each page, we will crawl every properties and get the html snapshot stored in the object store
        """

        if not self.page_count:
            # Nếu không thấy page_count phải alert ngay lập tức và set logic cho page_count để ưu tiên crawler chạy bình thường
            page_count = 99999

        for page in range(1, self.page_count +1):
            try:
                soup_properties = []
                response = requests.get(f"https://batdongsan.com.vn/nha-dat-cho-thue/p{page}", impersonate='chrome')
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a', class_="js__product-link-for-product-id")

                if len(links) == 0 and page_count == 99999:
                    # Ghi nhận rẳng không tìm thấy item nào ở trong page đó
                    break

                if response.status_code == 403:
                    # Ghi nhận response code 403
                    continue
                else:
                    # Ghi nhận response code
                    pass

                for link in links:
                    href = str(link.get('href'))
                    if href.startswith('/'):
                        href = 'https://batdongsan.com.vn' + href
                    response_property= requests.get(href, impersonate = 'chrome')

                    if response_property.status_code == 403:
                        # Ghi nhận response code 403
                        continue
                    else:
                        # Ghi nhận response code
                        pass
                    
                    soup_properties.append(BeautifulSoup(response_property.text, 'html.parser'))

                # Đây là nơi mà ta tiếp tục gửi snapshot tới minio với soup là page kết quả của search và soup_properties là nhà cho thuê

            except Exception as e:
                print("Có lỗi xảy ra! ", e)
                continue

    def getSomething(self) -> None:
        pass