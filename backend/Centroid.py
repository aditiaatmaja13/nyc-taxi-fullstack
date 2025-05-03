import geopandas as gpd
import pandas as pd

# Load the GeoJSON file
gdf = gpd.read_file("taxi_zones.geojson")

# Calculate centroids
gdf['lat'] = gdf.centroid.y
gdf['lng'] = gdf.centroid.x

# Output only LocationID, lat, lng
df = gdf[['LocationID', 'lat', 'lng']]
df.to_csv("zone_centroids.csv", index=False)
