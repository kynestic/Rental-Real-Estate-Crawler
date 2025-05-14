import requests, time, math, json
import numpy as np
from bs4 import BeautifulSoup
import random

chrome_versions = [
    "120.0.0.0", "119.0.0.0", "118.0.0.0", "117.0.0.0", "116.0.0.0",
    "115.0.0.0", "114.0.0.0", "113.0.0.0", "112.0.0.0", "111.0.0.0"
]

# Danh sách các phiên bản hệ điều hành
os_versions = [
    "Windows NT 10.0; Win64; x64", "Macintosh; Intel Mac OS X 10_15_7", 
    "X11; Ubuntu; Linux x86_64", "Windows NT 6.1; WOW64; Trident/7.0"
]

# Hàm để tạo một header ngẫu nhiên
def generate_header():
    user_agent = f"Mozilla/5.0 ({random.choice(os_versions)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.choice(chrome_versions)} Safari/537.36"
    header = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": f'"Not_A Brand";v="8", "Chromium";v="{random.choice(chrome_versions)}", "Google Chrome";v="{random.choice(chrome_versions)}"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": user_agent,
    }
    return header

RUN_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "DNT": "1",  # Do Not Track
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Sec-Ch-Ua": '"Chromium";v="123", "Not-A.Brand";v="8", "Google Chrome";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

proxy = 'http://brd-customer-hl_4bb26e51-zone-residential_proxy1:d9b439r3cop7@brd.superproxy.io:33335'

proxies = {
    'http': proxy,
    'https': proxy,
}

class ZillowCrawler():
    def __init__(self, srch_terms) -> None:
        self.srch_terms = srch_terms

    def throtling(self):
        time.sleep(np.random.randint(2,6))

    def getCoordinate(self):
        chrome_version = np.random.randint(120, 132)
        BASE_HEADERS = {
                'user-agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version}.0.0.0 Safari/537.36',
                'origin':'https://www.zillow.com',
        }

        url_map = f'https://www.zillow.com/{self.srch_terms}/rentals/?'
        response = requests.get(url_map, headers=BASE_HEADERS)
        bs4ob = BeautifulSoup(response.text, 'lxml')
        scripts = bs4ob.find_all("script")
        #Get the map coordinates
        coords = [x.text for x in scripts if "window.mapBounds" in x.text]
        start = coords[0].index("mapBounds")
        end = start + coords[0][start:].index(";\n")
        mapcords = coords[0][start:end].split(" = ")[1]
        map_coords = json.loads(mapcords)

        #Region ID and Type is next
        region = [x.text for x in scripts if "regionId" in x.text]
        start = region[0].index("regionId")
        end = start + region[0][start:].index("]") - 1
        regionID, regionType = region[0][start:end].split(",")
        regionID = int("".join([x for x in regionID if x.isnumeric()]))
        regionType = int("".join([x for x in regionType if x.isnumeric()]))
        
        return regionID, regionType, map_coords


    def dataProcessing(self, json_input, filter = None):
        results = json_input.get("cat1")["searchResults"]["listResults"]
        return results

    def setMaxPage(self, json_input):
        max_output = json_input["cat1"]["searchList"]["totalResultCount"]
        item_count = json_input["cat1"]["searchList"]["resultsPerPage"]
        return max_output, item_count


    def requestForData(self, filter = None):
        regionID = self.regionID
        regionType = self.regionType
        map_coords = self.map_coords
        subparams = None
        if not filter:
            subparams = {
                "pagination": {"currentPage": 1},
                "isMapVisible": True,
                "mapBounds": map_coords,
                "mapZoom": 13,
                "usersSearchTerm": str(self.srch_terms),
                "regionSelection": [
                    {
                    "regionId": regionID,
                    "regionType": regionType
                    }
                ],
                "filterState": {
                    "sortSelection": {
                    "value": "priorityscore"
                    },
                    "isForRent": {
                    "value": True
                    },
                    "isForSaleByAgent": {
                    "value": False
                    },
                    "isForSaleByOwner": {
                    "value": False
                    },
                    "isNewConstruction": {
                    "value": False
                    },
                    "isComingSoon": {
                    "value": False
                    },
                    "isAuction": {
                    "value": False
                    },
                    "isForSaleForeclosure": {
                    "value": False
                    },
                    "isMultiFamily": {
                    "value": False
                    },
                    "isLotLand": {
                    "value": False
                    },
                    "isManufactured": {
                    "value": False
                    },
                    "isRoomForRent": {
                    "value": True
                    }
                },
                "isListVisible": True
            }
        else:
            subparams = {
                "pagination": {"currentPage": 1},
                "isMapVisible": True,
                "mapBounds": map_coords,
                "mapZoom": 13,
                "usersSearchTerm": str(self.srch_terms),
                "regionSelection": [
                    {
                        "regionId": regionID,
                        "regionType": regionType
                    }
                ],
                "filterState": {
                    "sortSelection": {
                        "value": "priorityscore"
                    },
                    "isForRent": {
                        "value": True
                    },
                    "isForSaleByAgent": {
                        "value": False
                    },
                    "isForSaleByOwner": {
                        "value": False
                    },
                    "isNewConstruction": {
                        "value": False
                    },
                    "isComingSoon": {
                        "value": False
                    },
                    "isAuction": {
                        "value": False
                    },
                    "isForSaleForeclosure": {
                        "value": False
                    },
                    "isMultiFamily": {
                        "value": False
                    },
                    "isLotLand": {
                        "value": False
                    },
                    "isManufactured": {
                        "value": False
                    },
                    "isRoomForRent": {
                        "value": True
                    },
                    filter: { 
                        "value": True
                    }
                },
                "isListVisible": True
            }

        params = {
            "searchQueryState": subparams,
            "wants": {"cat1": ["listResults", "mapResults"], "cat2": ["total"]},
            "requestId": np.random.randint(2, 10),
            "isDebugrequest":"false"
        }

        self.throtling()

        response = None
        url_search = "https://www.zillow.com/async-create-search-page-state"
        payload = { 'api_key': '526c4b11ec175cac8d7aee7703a840c0', 'url': url_search}
        response = requests.put('https://api.scraperapi.com/', params=payload, json=params)

        json_raw = response.json()
        data_list = self.dataProcessing(json_raw)


        max_output, item_count = self.setMaxPage(json_raw)
        max_page = None
        if max_output <= item_count:
            max_page = 1
        else:
            max_page = math.ceil(max_output/item_count)

        for curr_page in range(2, max_page + 1):
            subparams["pagination"]["currentPage"] = curr_page
            params = {
                "searchQueryState": subparams,
                "wants": {"cat1": ["listResults", "mapResults"], "cat2": ["total"]},
                "requestId": np.random.randint(2, 10),
                "isDebugrequest":"false"
            }
            self.throtling()
            url_search = "https://www.zillow.com/async-create-search-page-state"
            response = None
            payload = { 'api_key': '526c4b11ec175cac8d7aee7703a840c0', 'url': url_search}
            response = requests.put('https://api.scraperapi.com/', params=payload, json=params)
                

            data = response.json()
            data = self.dataProcessing(data)
            data_list.extend(data)
        
        unique_data = {}
        for d in data_list:
            zpid = d.get('zpid')
            if zpid not in unique_data:
                unique_data[zpid] = d
        
        if not filter:
            return unique_data
        else:
            return unique_data.keys()
        


    def getData(self):
        rental_attributes = [
            "isBasementFinished",
            "onlyRentalLargeDogsAllowed",
            "onlyRentalSmallDogsAllowed",
            "onlyRentalCatsAllowed",
            "onlyRentalNoPets",
            "hasAirConditioning",
            "hasPool",
            "isWaterfront",
            "onlyRentalParkingAvailable",
            "onlyRentalInUnitLaundry",
            "elevatorAccessAvailable",
            "onlyRentalShortTermLease",
            "areUtilitiesIncluded",
        ]

        self.regionID, self.regionType, self.map_coords = self.getCoordinate()
        self.final_data = self.requestForData()

        for item in rental_attributes:
            filter_data = self.requestForData(item) 
            for zpid, apt in self.final_data.items():  
                if zpid in filter_data:
                    if "factsAndFeatures" not in apt:
                        apt["factsAndFeatures"] = {}  # Nếu chưa có factsAndFeatures thì tạo mới
                    apt["factsAndFeatures"][item] = True
                else:
                    if "factsAndFeatures" not in apt:
                        apt["factsAndFeatures"] = {}  # Nếu chưa có factsAndFeatures thì tạo mới
                    apt["factsAndFeatures"][item] = False

        return self.final_data

