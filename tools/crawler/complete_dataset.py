import osmnx as ox
from geopy.distance import geodesic

def find_nearest_place(lat, lon, tags, place_type):
    radius = 500  # Bắt đầu từ 500m
    max_radius = 10000  # Tối đa 10km

    while radius <= max_radius:
        try:
            gdf = ox.geometries_from_point((lat, lon), tags=tags, dist=radius)
            if not gdf.empty:
                gdf = gdf[gdf.geometry.notnull()]
                gdf['distance'] = gdf.centroid.apply(lambda x: geodesic((lat, lon), (x.y, x.x)).meters)
                nearest = gdf.sort_values(by='distance').iloc[0]
                return {
                    'type': place_type,
                    'name': nearest.get('name', 'unknown'),
                    'distance_m': round(nearest['distance'], 2),
                    'coordinates': (nearest.geometry.centroid.y, nearest.geometry.centroid.x)
                }
        except Exception as e:
            print(f"Error while searching {place_type}: {e}")

        radius += 500

    return {"type": place_type, "error": f"No {place_type} found within {max_radius}m"}

# ----------------------------
# 3 HÀM RIÊNG CHO MỖI ĐỊA ĐIỂM
# ----------------------------

def nearest_hospital(lat, lon):
    tags = {"amenity": "hospital"}
    return find_nearest_place(lat, lon, tags, "hospital")

def nearest_park(lat, lon):
    tags = {"leisure": "park"}
    return find_nearest_place(lat, lon, tags, "park")

def nearest_supermarket(lat, lon):
    tags = {"shop": "supermarket"}
    return find_nearest_place(lat, lon, tags, "supermarket")

lat = 34.053032  # ví dụ San Francisco
lon = -118.268575

print(nearest_hospital(lat, lon))
print(nearest_park(lat, lon))
print(nearest_supermarket(lat, lon))


import pandas as pd
import time

df = pd.read_csv(r"D:\CODING\DATN\data\raw\tmp.csv")  # thay bằng tên file của bạn

for idx, row in df.iterrows():
    lat, lon = row['lat'], row['lon']
    
    print(f"Processing row {idx+1}/{len(df)}: ({lat}, {lon})")

    try:
        park_info = nearest_park(lat, lon)
        hospital_info = nearest_hospital(lat, lon)
        market_info = nearest_supermarket(lat, lon)

        df.at[idx, 'distance_to_park'] = park_info.get('distance_m', None)
        df.at[idx, 'distance_to_hospital'] = hospital_info.get('distance_m', None)
        df.at[idx, 'distance_to_supermarket'] = market_info.get('distance_m', None)

        # Đợi một chút giữa các request để tránh bị giới hạn
        time.sleep(1)
        
    except Exception as e:
        print(f"Error at row {idx}: {e}")

# Lưu lại file mới
df.to_csv(r"D:\CODING\DATN\data\raw\tmp1.csv", index=False)
print("Finished writing distances to output_with_distances.csv")


