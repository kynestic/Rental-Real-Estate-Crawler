import logging
from bs4 import BeautifulSoup
import requests
import time
from typing import Union
import re
import time
import numpy as np
import random

def update_user_agent_version():
    # Headers ban đầu
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="122", "Chromium";v="122"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'iframe',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
        'referer': 'www.apartments.com',
        'origin':'https://www.apartments.com',
    }
    
    # Tạo một số phiên bản ngẫu nhiên giữa 100 và 130 cho Chrome
    new_version = random.randint(100, 130)
    
    # Cập nhật phiên bản trong user-agent
    user_agent = headers['user-agent']
    updated_user_agent = user_agent.replace('Chrome/122.0.0.0', f'Chrome/{new_version}.0.0.0')
    
    # Cập nhật lại header với user-agent mới
    headers['user-agent'] = updated_user_agent
    
    return headers

class ApartmentCrawler():
    def __init__(self, zipcode) -> None:
        self.zipcode = zipcode

    def soupHandling(self, result, property_type = None, filter = None):
        # time.sleep(np.random.rand())
        studio = listingid = price = beds = sqft = baths = pets = url = addy = current_time = extrafun = None
        img = []
        
        listings = []
        studio = False
        #Set the outer loop over each card returned. 
        for card in result.find_all("article", class_=lambda x: x and x.startswith("placard")):
            img_object = card.find_all('img')
            
            for item in img_object:
                img.append(item['src'])
            # Time of pull
            current_time = time.strftime("%m-%d-%Y_%H-%M-%S")

            #Grab the id
            listingid = card.get("data-listingid")
            addy = card.get("data-streetaddress")
            
            #First grab the link
            if card.get("data-url"):
                url = card.get("data-url")
                #If the listingid wasn't in the metadata (sometimes happens)
                #Pull it from the end of the URL
                if not listingid:
                    listingid = url.split("/")[-2]
            else:
                print(f"missing url and id for card")
                continue

            #grab the property info
            for search in card.find_all("div", class_="propertyInfo"):
                if property_type or filter:
                    continue
                #Grab price
                text = search.text
                price_match = re.search(r'\$\s?([\d,]+)', text)
                price = int(price_match.group(1).replace(',', '')) if price_match else None

                # Tìm số phòng ngủ (Beds)
                bed_match = re.search(r'(\d+)\s*Beds?', text, re.IGNORECASE)
                beds = int(bed_match.group(1)) if bed_match else (0 if re.search(r'Studio', text, re.IGNORECASE) else None)

                if not beds:
                    studio = bool(re.search(r'\bStudio\b', text, re.IGNORECASE))

                # Tìm số phòng tắm (Baths)
                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*Baths?', text, re.IGNORECASE)
                baths = float(bath_match.group(1)) if bath_match else None

                # Tìm diện tích (sq ft)
                sqft_match = re.search(r'([\d,]+)\s*sq\s*ft', text, re.IGNORECASE)
                sqft = int(sqft_match.group(1).replace(',', '')) if sqft_match else None
            if property_type != None or filter != None:
                listings.append(str(listingid))
            else:
                listing = {
                    'listingid' : listingid,
                    'source' : "www.apartment.com",
                    'price' : price,
                    'studio': studio,
                    'beds' : beds,
                    'sqft' : sqft,
                    'baths' : baths,
                    'img': img,
                    'url' : url,
                    'addy' : addy,
                    'current_time': current_time
                }
                listings.append(listing)
            studio = listingid = price = beds = sqft = baths = pets = url = addy = current_time = None
            img = []
        return listings
            

    def requestForData(self, property_type = None ,filter=None)->list:
        print(property_type, filter)
        zipcode = self.zipcode
        url = None
        if not property_type and not filter:
            url = f"https://www.apartments.com/los-angeles-ca-{zipcode}/1"
        elif property_type:
            url = f"https://www.apartments.com/{property_type}/los-angeles-ca-{zipcode}/1"
        else:
            url = f"https://www.apartments.com/los-angeles-ca-{zipcode}/{filter}/1"
        print(url)
        

        response = requests.get(url, headers=update_user_agent_version())

        #Just in case we piss someone off
        if response.status_code != 200:
            # If there's an error, log it and return no data for that site
            print(f'Status code: {response.status_code}')
            print(f'Reason: {response.reason}')
            return None

        #Get the HTML
        bs4ob = BeautifulSoup(response.text, 'lxml')
        element = bs4ob.select_one("#placardContainer > ul > li:nth-child(41) > p > span")
        page_count = None
        if element:
            text = element.get_text(strip=True)
            numbers = re.findall(r'\d+', text)
            if numbers:
                page_count = int(numbers[-1])
        # Isolate the property-list from the expanded one (I don't want the 3 mile
        # surrounding.  Just the neighborhood)
        nores = bs4ob.find_all("div", class_="no-results")
        result = bs4ob.find("div", id="placardContainer")
        if not nores:
            if not result:
                print("Not found results returned on apartments.  Moving to next site")
                return []
                
        else:
            print("No listings returned on apartments.  Moving to next site")
            return []

        listings = self.soupHandling(result, property_type=property_type, filter=filter)

        if not page_count or page_count <= 1:
            return listings
        else:
            for page in range(2, page_count):
                url = None
                if not property_type and not filter:
                    url = f"https://www.apartments.com/los-angeles-ca-{zipcode}/{page}"
                elif property_type:
                    url = f"https://www.apartments.com/{property_type}/los-angeles-ca-{zipcode}/{page}"
                else:
                    url = f"https://www.apartments.com/los-angeles-ca-{zipcode}/{filter}/{page}"
                print(url)
                response = requests.get(url, headers=update_user_agent_version())
                if response.status_code != 200:
                    # If there's an error, log it and return no data for that site
                    print(f'Status code: {response.status_code}')
                    print(f'Reason: {response.reason}')
                    return None

                nores = bs4ob.find_all("div", class_="no-results")
                result = bs4ob.find("div", id="placardContainer")
                if not nores:
                    if not result:
                        print("Not found results returned on apartments.  Moving to next site")
                        return []
                        
                else:
                    print("No listings returned on apartments.  Moving to next site")
                    return []

                listings.extend(self.soupHandling(result, property_type=property_type, filter=filter))
            
            return listings



    def money_launderer(self, price:list)->float:
        """[Strips dollar signs and comma from the price]

        Args:
            price (list): [list of prices as strs]

        Returns:
            price (list): [list of prices as floats]
        """	
        if isinstance(price, str):
            return float(price.replace("$", "").replace(",", ""))
        return price

    def getData(self):
        """[Outer scraping function to set up request pulls]

        Args:
            neigh (Union[str,int]): Neighborhood or zipcode searched
            source (str): What site is being scraped
            Propertyinfo (dataclass): Custom data object
            srch_par (tuple): Tuple of search parameters

        Returns:
            property_listings (list): List of dataclass objects
        """ 
        property_types = [
            "apartments",
            "houses",
            "townhomes",
            "condos"
        ]

        filters = [
            "pet-friendly-cat",
            "pet-friendly-dog",
            "pet-friendly",
            "washer-dryer",
            "air-conditioning",
            "utilities-included",
            "dishwasher",
            "parking",
            "garage",
            "laundry-facilities",
            "washer_dryer-hookup",
            "pool",
            "fitness-center",
            "gated",
            "duplex",
            "recent-build",
            "under-50-units",
            "walk-in-closets"
        ]
        final_data = self.requestForData()

        for item in property_types:
            
            filter = self.requestForData(property_type=item)
            #code xử lý final_data
            for rental in final_data:

                if rental["listingid"] in filter:
                    rental[item] = True
                else:
                    rental[item] = False


        for item in filters:
            filter = self.requestForData(filter=item)
            #code xử lý final_data
            for rental in final_data:
                if rental['listingid'] in filter:
                    rental[item] = True
                else:
                    rental[item] = False
        
        return final_data

        


