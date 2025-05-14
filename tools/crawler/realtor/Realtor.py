from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from enum import Enum
import time
import numpy as np
import json
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
_SEARCH_HOMES_DATA_BASE = """{
    pending_date
    listing_id
    property_id
    href
    list_date
    status
    last_sold_price
    last_sold_date
    list_price
    list_price_max
    list_price_min
    price_per_sqft
    tags
    details {
        category
        text
        parent_category
    }
    pet_policy {
        cats
        dogs
        dogs_small
        dogs_large
        __typename
    }
    units {
        availability {
          date
          __typename
        }
        description {
          baths_consolidated
          baths
          beds
          sqft
          __typename
        }
        list_price
        __typename
    }
    flags {
        is_contingent
        is_pending
        is_new_construction
    }
    description {
        type
        sqft
        beds
        baths_full
        baths_half
        lot_sqft
        year_built
        garage
        type
        name
        stories
        text
    }
    source {
        id
        listing_id
    }
    hoa {
        fee
    }
    location {
        address {
            street_direction
            street_number
            street_name
            street_suffix
            line
            unit
            city
            state_code
            postal_code
            coordinate {
                lon
                lat
            }
        }
        county {
            name
            fips_code
        }
        neighborhoods {
            name
        }
    }
    tax_record {
        public_record_id
    }
    primary_photo(https: true) {
        href
    }
    photos(https: true) {
        href
        tags {
            label
        }
    }
    advertisers {
        email
        broker {
            name
            fulfillment_id
        }
        type
        name
        fulfillment_id
        builder {
            name
            fulfillment_id
        }
        phones {
            ext
            primary
            type
            number
        }
        office {
            name
            email
            fulfillment_id
            href
            phones {
                number
                type
                primary
                ext
            }
            mls_set
        }
        corporation {
            specialties
            name
            bio
            href
            fulfillment_id
        }
        mls_set
        nrds_id
        rental_corporation {
            fulfillment_id
        }
        rental_management {
            name
            href
            fulfillment_id
        }
    }
    """


HOME_FRAGMENT = """
fragment HomeData on Home {
    property_id
    nearbySchools: nearby_schools(radius: 5.0, limit_per_level: 3) {
        __typename schools { district { __typename id name } }
    }
    taxHistory: tax_history { __typename tax year assessment { __typename building land total } }
    monthly_fees {
        description
        display_amount
    }
    one_time_fees {
        description
        display_amount
    }
    parking {
        unassigned_space_rent
        assigned_spaces_available
        description
        assigned_space_rent
    }
    terms {
        text
        category
    }
}
"""

HOMES_DATA = """%s
                nearbySchools: nearby_schools(radius: 5.0, limit_per_level: 3) {
                            __typename schools { district { __typename id name } }
                        }
                monthly_fees {
                    description
                    display_amount
                }
                one_time_fees {
                    description
                    display_amount
                }
                parking {
                    unassigned_space_rent
                    assigned_spaces_available
                    description
                    assigned_space_rent
                }
                terms {
                    text
                    category
                }
                taxHistory: tax_history { __typename tax year assessment { __typename building land total } }
                estimates {
                    __typename
                    currentValues: current_values {
                        __typename
                        source { __typename type name }
                        estimate
                        estimateHigh: estimate_high
                        estimateLow: estimate_low
                        date
                        isBestHomeValue: isbest_homevalue
                    }
                }
}""" % _SEARCH_HOMES_DATA_BASE

SEARCH_HOMES_DATA = """%s
current_estimates {
    __typename
    source {
        __typename
        type
        name
    }
    estimate
    estimateHigh: estimate_high
    estimateLow: estimate_low
    date
    isBestHomeValue: isbest_homevalue
}
}""" % _SEARCH_HOMES_DATA_BASE

GENERAL_RESULTS_QUERY = """{
                            count
                            total
                            results %s
                        }""" % SEARCH_HOMES_DATA
class ListingType(Enum):
    FOR_SALE = "FOR_SALE"
    FOR_RENT = "FOR_RENT"
    PENDING = "PENDING"
    SOLD = "SOLD"

class RealtorCrawler():
    def __init__(self, postal_code) -> None:
        session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=4, status_forcelist=[429, 403], allowed_methods=frozenset(["GET", "POST"])
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "accept": "application/json, text/javascript",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json",
                "origin": "https://www.realtor.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "rdc-ab-tests": "commute_travel_time_variation:v1",
                "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            }
        )

        self.session = session
        self.postal_code = postal_code

    def requestForData(self, payload):
        # time.sleep(np.random.rand())
        response = self.session.post("https://www.realtor.com/api/v1/rdc_search_srp?client_id=rdc-search-new-communities&schema=vesta", json=payload)
        response_json = response.json()
        search_key = "home_search" if "home_search" in payload['query'] else "property_search"

        if (
                response_json is None
                or "data" not in response_json
                or response_json["data"] is None
                or search_key not in response_json["data"]
                or response_json["data"][search_key] is None
                or "results" not in response_json["data"][search_key]
            ):
                return {"total": 0, "properties": []}

        

        properties_list = response_json["data"][search_key]["results"]
        total_properties = response_json["data"][search_key]["total"]
        

        return {
                "total": total_properties,
                "properties": properties_list,
            }

    def getData(self):
        postal_code = self.postal_code
        is_foreclosure = "foreclosure: false"
        listing_type = ListingType.FOR_RENT
        date_param = ""
        property_type_param = ""
        pending_or_contingent_param = (
                "or_filters: { contingent: true, pending: true }" if listing_type == ListingType.PENDING else ""
            )
        sort_param = ""
        query = """query Home_search(
                                    $city: String,
                                    $county: [String],
                                    $state_code: String,
                                    $postal_code: String
                                    $offset: Int,
                                ) {
                                    home_search(
                                        query: {
                                            %s
                                            city: $city
                                            county: $county
                                            postal_code: $postal_code
                                            state_code: $state_code
                                            status: %s
                                            %s
                                            %s
                                            %s
                                        }
                                        bucket: { sort: "fractal_v1.1.3_fr" }
                                        %s
                                        limit: 200
                                        offset: $offset
                                    ) %s
                                }""" % (
                    is_foreclosure,
                    listing_type.value.lower(),
                    date_param,
                    property_type_param,
                    pending_or_contingent_param,
                    sort_param,
                    GENERAL_RESULTS_QUERY,
                )

        search_variables = {
            "offset": 0,
            "postal_code": postal_code,
            "foreclosure": False,
        }

        payload = {
                "query": query,
                "variables": search_variables,
        }


        result = self.requestForData(payload)
        total = result["total"]
        homes = result["properties"]

        with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        self.requestForData,
                        payload = {
                            "query": query,
                            "variables": {
                                "offset": i,
                                "postal_code": postal_code,
                                "foreclosure": False,
                            },
                        },
                    )
                    for i in range(
                        200,
                        min(total, 10000),
                        200,
                    )
                ]

                for future in as_completed(futures):
                    data = future.result()
                    data = data.get("properties")
                    homes.extend(data)
        for item in homes:
            item['source'] = 'www.realtor.com'
        return homes


def getImageLink(url):
    import requests
    response = requests.get(str(url),headers={
                    "accept": "application/json, text/javascript",
                    "accept-language": "en-US,en;q=0.9",
                    "cache-control": "no-cache",
                    "content-type": "application/json",
                    "origin": "https://www.realtor.com",
                    "pragma": "no-cache",
                    "priority": "u=1, i",
                    "rdc-ab-tests": "commute_travel_time_variation:v1",
                    "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                })
    print(response.status_code)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    element = soup.find('div', id='main-container')
    img_list = element.find_all('img')
    img = []
    for item in img_list:
        data = item['src']
        if data:
            img.append(data)
        data = item.get('data-src')
        if data:
            img.append(data)
     
    return img





    





