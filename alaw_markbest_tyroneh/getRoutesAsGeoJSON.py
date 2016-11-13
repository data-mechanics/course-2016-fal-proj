url = 'http://wsgw.mass.gov/data/gispub/shape/state/mbtabus.zip'

# Get the data from the server.
response = urllib.request.urlopen(url)

# Unzip the file into the working directory.
zip_ref = zipfile.ZipFile(io.BytesIO(response.read()))
zip_ref.extractall("./")
zip_ref.close()

# Read the file into the Python shapefile library.
sf = shapefile.Reader("MBTABUSROUTES_ARC")

# Function to reverse the Lambert projection done on the route data and convert to latitude/longitude.
reverseCoordinateProjection = pyproj.Proj(proj = 'lcc', datum = 'NAD83', lat_1 = 41.71666666666667, lat_2 = 42.68333333333333,lat_0 = 41.0, lon_0 = -71.5, x_0 = 200000.0, y_0 = 750000.0)

# List comprehension to pull out route coordinates and related data in GeoJSON format.
geoJsonList = [
{
    "type":"Feature",
    "geometry":{
     "type":"Multipoint",
     "coordinates": [reverseCoordinateProjection(p[0], p[1], inverse = True) for p in x.shape.points]},
    "properties":{
     "route_name": x.record[8],
     "route_id": x.record[1],
         "direction":x.record[6]
    }
}
for x in [i for i in sf.shapeRecords()]]

