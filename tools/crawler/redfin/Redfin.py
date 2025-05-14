import logging
import json
from bs4 import BeautifulSoup
import numpy as np
import requests
import time
from enum import Enum
import math

class RentalFilter(Enum):
    HAS_POOL = "has-pool"
    DOGS_ALLOWED = "dogs-allowed"
    CATS_ALLOWED = "cats-allowed"
    AIR_CONDITIONING = "air-conditioning"
    WASHER_DRYER_IN_UNIT = "washer-dryer-in-unit"
    HAS_LAUNDRY_FACILITY = "has-laundry-facility"
    HAS_LAUNDRY_HOOKUPS = "has-laundry-hookups"
    HAS_DISHWASHER = "has-dishwasher"
    HAS_PARKING = "has-parking"
    UTILITIES_INCLUDED = "utilities-included"
    IS_FURNISHED = "is-furnished"
    HAS_SHORT_TERM_LEASE = "has-short-term-lease"
    IS_SENIOR_LIVING = "is-senior-living"
    IS_INCOME_RESTRICTED = "is-income-restricted"

def money_launderer(price:int)->str:
    """[Formats price to a single decimal k format.  This is redfins weird encoding nonsense]

    Args:
        price (int): [price as an int]

    Returns:
        price (str): [price as a str]
    """
    if isinstance(price, int):
        sprice = str(price)
        if len(sprice) < 4:
            fprice = sprice
        elif sprice[1] != "0":
            fprice = sprice[0] + "." + sprice[1] + "k"
        else:
            fprice = sprice[0] + "k"
        return fprice
    
    else:
        return price

#Notes
# https://github.com/ryansherby/RedfinScraper/blob/main/redfin_scraper/core/redfin_scraper.py


class RedfinCrawler():
    def __init__(self, postal_code) -> None:
        self.postal_code = postal_code

    def handleSoup(self, results, sub_path):
        # time.sleep(np.random.rand())
        listingid = price = beds = sqft = baths = pets = url = addy = current_time = lat = long = None
        img = []
        property_listings = []
        #Set the outer loop over each card returned. 
        for card in results.find_all("div", id=lambda x: x and x.startswith("MapHomeCard")):
            img_list = card.find_all('img')
            for item in img_list:
                img.append(item['src'])
            for subsearch in card.find_all("script", {"type":"application/ld+json"}):
                listinginfo = json.loads(subsearch.text)
                url = listinginfo[0].get("url")
                listingid = url.split("/")[-1]
                if sub_path:
                    continue
                addy = listinginfo[0].get("name")
                lat = float(listinginfo[0]["geo"].get("latitude"))
                long = float(listinginfo[0]["geo"].get("longitude"))
                beds = listinginfo[0].get("numberOfRooms")
                if "-" in beds: 
                    beds = float(beds.split("-")[-1])
                elif "," in beds: 
                    beds = float(beds.split(",")[-1])
                else:
                    beds = float(beds)
                    
                if "value" in listinginfo[0]["floorSize"].keys():
                    sqft = listinginfo[0].get("floorSize")["value"]
                    if "," in sqft:
                        sqft = sqft.replace(",", "")
                    sqft = float("".join(x for x in sqft if x.isnumeric()))
                price = float("".join(x for x in listinginfo[1]["offers"]["price"] if x.isnumeric()))
        
            # Time of pull
            current_time = time.strftime("%m-%d-%Y_%H-%M-%S")

            #Bathrooms weren't in the json.  So we'll grab those manually
            for subsearch in card.find_all("span", class_=lambda x: x and "bath" in x):
                if sub_path:
                    break
                baths = subsearch.text
                baths = float("".join(x for x in baths if x.isnumeric() or x == "."))
                break
            
            listing = {}
            
            if not sub_path:
                listing = {
                    "listingid": listingid,   
                    "source": "www.redfin.com",
                    "price": price,    
                    "beds": beds,       
                    "sqft": sqft,
                    "studio": False if beds != 0 else True ,     
                    "baths": baths,
                    'img': img,     
                    "lat": lat,
                    "long": long,
                    "url": url,		
                    "addy": addy,
                    "current_time": current_time,    
                }
            else:
                listing = listingid

            property_listings.append(listing)
            listingid = price = beds = sqft = baths = pets = url = addy = current_time = lat = long = None
            img = []
        logging.info(f'{len(property_listings)} listings returned from redfin')
        return property_listings

    def requestForData(self, sub_path = None):
        postal_code = self.postal_code

        if sub_path:
            sub_path = '/filter/' + sub_path

        chrome_version = np.random.randint(120, 132)
        INT_HEADERS = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer': 'www.redfin.com',
            'sec-ch-ua': f"'Google Chrome';v={chrome_version}, 'Not-A.Brand';v='8', 'Chromium';v={chrome_version}",
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version}.0.0.0 Safari/537.36',
        }

        url_search = f'https://www.redfin.com/zipcode/{postal_code}/apartments-for-rent'
        if sub_path:
            url_search += sub_path
        response = requests.get(url_search, headers = INT_HEADERS)
        bs4ob = BeautifulSoup(response.text, 'lxml')
        hcount = bs4ob.find("div", class_="homes summary reversePosition")
        first_number = None
        after_span_number = None
        span_text = None
        lcount = None
        if hcount:
            parts = hcount.contents

            
            found_span = False
            for item in parts:
                if isinstance(item, str):
                    clean_text = item.strip()
                    if not found_span and clean_text:
                        num_str = ''.join(x for x in clean_text if x.isnumeric())
                        if num_str:
                            first_number = int(num_str)
                    elif found_span and clean_text:
                        num_str = ''.join(x for x in clean_text if x.isnumeric())
                        if num_str:
                            after_span_number = int(num_str)
                elif item.name == "span":
                    found_span = True
                    span_text = item.get_text(strip=True)
            lcount = first_number

        else:
            print("No count found on redfin. Moving to next site.")
            
        if not lcount:
            print(response.status_code)
            print("No listings returned on Redfin.  Moving to next site")
            return []

        if lcount > 0:
            final_data = []
            results = bs4ob.find("div", class_="PhotosView reversePosition widerHomecardsContainer")
            final_data.extend(self.handleSoup(results, sub_path))
            count = math.ceil(lcount/40)
            for i in range(2, count + 1):
                url_search = f'https://www.redfin.com/zipcode/{postal_code}/apartments-for-rent'
                if sub_path:
                    url_search += sub_path
                curr_page = f'/page-{i}'
                url_search += curr_page
                response = requests.get(url_search, headers = INT_HEADERS)
                results = bs4ob.find("div", class_="PhotosView reversePosition widerHomecardsContainer")
                if results:
                    if results.get("data-rf-test-id") =='photos-view':
                        final_data.extend(self.handleSoup(results, sub_path))
                        
                    else:
                        print("The soups hath failed you")		
            return final_data
        else:
            print(response.status_code)
            print("No listings returned on Redfin.  Moving to next site")
            return []

    def getData(self):
        filter = [
        "has-pool",
        "dogs-allowed",
        "cats-allowed",
        "air-conditioning",
        "washer-dryer-in-unit",
        "has-laundry-facility",
        "has-laundry-hookups",
        "has-dishwasher",
        "has-parking",
        "utilities-included",
        "is-furnished",
        "has-short-term-lease",
        "is-senior-living",
        "is-income-restricted"

        ]
        final_data = self.requestForData()

        # Lặp qua từng filter
        for filter_item in filter:
            # Lấy danh sách các id căn hộ thỏa mãn filter này
            allowed_ids = self.requestForData(filter_item)
            
            # Thêm trường thông tin cho mỗi item
            final_data = [
                {**item, filter_item: item["listingid"] in allowed_ids}  # Thêm trường filter_item với True/False
                for item in final_data
            ]
        return final_data