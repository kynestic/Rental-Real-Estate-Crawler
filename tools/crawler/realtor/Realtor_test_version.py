from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from enum import Enum
from query import _SEARCH_HOMES_DATA_BAS, HOME_FRAGMENT, HOMES_DATA, GENERAL_RESULTS_QUERY

class ListingType(Enum):
    FOR_SALE = "FOR_SALE"
    FOR_RENT = "FOR_RENT"
    PENDING = "PENDING"
    SOLD = "SOLD"


session = None

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

if not session:
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


def handle_location(location, listing_type):
    params = {
        "input": location,
        "client_id": listing_type.value.lower().replace("_", "-"),
        "limit": "1",
        "area_types": "city,state,county,postal_code,address,street,neighborhood,school,school_district,university,park",
    }
    session = requests.session()
    response = session.get(
        "https://parser-external.geo.moveaws.com/suggest",
        params=params
    )

    response_json = response.json()

    result = response_json["autocomplete"]

    if not result:
        return None

    return result[0]

def general_search(variables: dict):
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
    
    payload = {
            "query": query,
            "variables": variables,
    }

    print('-----------------------')
    response = session.post("https://www.realtor.com/api/v1/rdc_search_srp?client_id=rdc-search-new-communities&schema=vesta", json=payload)
    response_json = response.json()
    search_key = "home_search" if "home_search" in query else "property_search"

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

def search(postal_code):
    search_variables = {
        "offset": 0,
        "postal_code": postal_code,
        "foreclosure": False,
    }

    result = general_search(search_variables)
    total = result["total"]
    homes = result["properties"]

    with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    general_search,
                    #offset i là
                    variables=search_variables | {"offset": i},
                    search_type='FOR_RENT'
                )
                for i in range(
                    200,
                    min(total, 10000),
                    200,
                )
            ]

            for future in as_completed(futures):
                homes.extend(future.result()["properties"])

    return homes
    
import json
json_data = json.dumps(search("90011"), ensure_ascii=False, indent=4)
with open('tmp.txt', 'w') as f:
    f.write(str(json_data))
    f.close()



    





